-module(vc_master_primary).
-include("include/records.hrl").
-behaviour(gen_server).

-export([init/1, start_link/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
	dc_id,
	secondary_master,       %
	secondary_master_node,  %
	secondary_master_status,% ready, initializing, undefined
	secondary_synchronizer, % every 5 seconds, the primary sends table updates to the secondary in addition to mnesia subscriber
	vc_meta_handler,        % cap data feeder
	vc_interval_handler,    % next interval scheduler
	vc_local_handler,       % handles local requests from nodes
	vc_remote_handler,      % handles remote requests from master and other d.c.
	process_monitor,        % every 10 seconds, monitor which processes are alive, and update state
	vc_dets_last_insert_id=0,
	vc_dets_last_sync_id=0
}).

start_link() ->
	{ok,_Pid} = gen_server:start_link({global, vc_master:master_name(primary)}, ?MODULE, [], []).

init([]) ->
	process_flag(trap_exit, true),
	MasterNodes = case application:get_env(vc_master, master_nodes) of
		undefined   -> [];
		{ok, Nodes} -> Nodes
	end,
	application:set_env(vc_master, master_nodes, lists:usort(MasterNodes ++ [node()])),
	State = init_as_primary_master(),	
	{ok, State}.

handle_call({update_secondary_status, Status}, {Pid,_Tag}=_From, State) ->
	error_logger:info_msg("vc_master_primary: <-MS, received seconcary master status: ~p",[Status]),
	vc_master:add_node(node(Pid)),
	SynchronizerTRef = if 
		Status == ready ->
			{Synchronizer, _} = start_sync_and_monitor_of_secondary(), 
			Synchronizer;
		true ->
			undefined	
	end,
	NewState = State#state{
		secondary_master=Pid,
		secondary_master_node= node(Pid),
		secondary_master_status= Status,
		secondary_synchronizer = SynchronizerTRef
	},
	{reply, ok, NewState};

handle_call(get_state, _From, State) ->
	{reply, State, State};

handle_call(Unexpected, From, State) ->
	error_logger:warning_msg("vc_master_primary: unexpected handle_call message ~p from ~p", [Unexpected, From]),
	{reply, ignored, State}.

handle_cast(monitor_processes, State) -> 
	SecondaryMasterPid = global:whereis_name(vc_master:master_name(secondary)),
	{SecondaryMasterNode, SecondaryMasterStatus} =  if 
		is_pid(SecondaryMasterPid) -> {node(SecondaryMasterPid), ready};
		true-> {undefined, undefined}
	end,
	NewState = State#state{
		secondary_master = SecondaryMasterPid,
		secondary_master_node = SecondaryMasterNode,
		secondary_master_status = SecondaryMasterStatus,
		vc_meta_handler = global:whereis_name(vc_master:handler_name(meta)),
		vc_interval_handler = global:whereis_name(vc_master:handler_name(interval)),
		vc_local_handler = global:whereis_name(vc_master:handler_name(local)),
		vc_remote_handler = global:whereis_name(vc_master:handler_name(remote))
	},
	case lists:member(undefined, tuple_to_list(NewState)) of
		true -> error_logger:warning_msg("vc_primary_master: Improper state. possibly secondary not started.~n ~p",[NewState]);
		false -> ok
	end,
	{noreply, NewState};

% this is called by mnesia trigger and timer
handle_cast(sync_secondary, State) when State#state.secondary_master_status == ready ->
	NewState = sync_secondary(State),
	{noreply, NewState};

handle_cast(sync_secondary, State) when State#state.secondary_master_status /= ready ->
	error_logger:warning_msg("vc_master_primary: Invalid secondary master status for sync: ~p", [State#state.secondary_master_status]),
	{noreply, State};

handle_cast(Unexpected, State) ->
	error_logger:warning_msg("vc_master_primary: unexpected handle_cast message ~p", [Unexpected]),
	{noreply, State}.

% {write,  NewRecord, ActivityId},  {delete, {Tab, key}, ActivityId}, {delete_object, OldRecord, ActivityId}
handle_info({mnesia_table_event, Event}, State) when State#state.secondary_master_status == ready ->
	% send all updateds of vc_meta/vc_interval to the secondary master when it is ready 
	LastInsertId = State#state.vc_dets_last_insert_id,
	NewLastInsertId = LastInsertId+1,
	dets:insert(vc_dets, [{NewLastInsertId, Event}]),
	gen_server:cast({global, vc_master:master_name(primary)}, sync_secondary),
	NewState=State#state{vc_dets_last_insert_id = NewLastInsertId},
	{noreply, NewState};

handle_info(Message={'DOWN', _MonitorRef, _Type, _Object, _Info}, State) -> % secondary master is down
	error_logger:warning_msg("vc_master_primary: Detected secondary master down. ~p", [Message]),
	vc_master:del_node( State#state.secondary_master_node ),
	stop_sync_and_monitor_of_secondary(State#state.secondary_synchronizer),
	{noreply, State#state{secondary_synchronizer=undefined}};
	
handle_info(Unexpected, State) ->
	error_logger:warn_msg("vc_master_primary: unexpected handle_info message ~p", [Unexpected]),
	{noreply, State}.

terminate(Reason, _State) ->
	error_logger:info_msg("vc_master_primary: stopping with reason, ~p", [Reason]),
	goodbye.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%-----------------------------------------------------------------------------------------------
% internal functions for PRIMARY 
%-----------------------------------------------------------------------------------------------
init_as_primary_master() ->
	{ok, DcId} = application:get_env(vc_master, dc_id),

	error_logger:info_msg("vc_master_primary: checking tables"),
	Tables = [vc_meta, vc_interval],
	vc_master:create_tables_if_not_exists(Tables),
	vc_master:wait_for_tables(Tables),

	error_logger:info_msg("vc_master_primary: trying start of the secondary master"),
	start_secondary_master(),
	
	error_logger:info_msg("vc_master_primary: starting 1 sec. timer of to monitor processes"),
	PrimaryMasterName = vc_master:master_name(primary),
	gen_server:cast({global,PrimaryMasterName}, monitor_processes),
	timer:apply_after(1000, gen_server, cast, [ {global,PrimaryMasterName}, monitor_processes]),
	{ok, MonitorTRef} = timer:apply_interval(10000, gen_server, cast, [ {global,PrimaryMasterName}, monitor_processes]),

	SecondaryMasterName = vc_master:master_name(secondary),
	catch gen_server:cast({global,SecondaryMasterName}, update_status),

	#state{
		dc_id = DcId,
		secondary_master = global:whereis_name(SecondaryMasterName),
		process_monitor = MonitorTRef
	}.

start_secondary_master() ->
	PrimaryMasterName = vc_master:master_name(primary),
	PrimaryMasterNode = node(global:whereis_name( PrimaryMasterName )),
	SecondaryMasterName = vc_master:master_name(secondary),
	case global:whereis_name( SecondaryMasterName ) of 
		Pid when is_pid(Pid) -> % secondary already started
			error_logger:info_msg("vc_master_primary: Start secondary master atttmpted, but it is already started"),
			Pid;
		undefined ->
			{ok, MasterNodes} = application:get_env(vc_master, master_nodes),
			PossibleSecMasterNodes = MasterNodes -- [PrimaryMasterNode],
			error_logger:warning_msg("vc_master_primary: Starting secondary master from ~p", [PossibleSecMasterNodes]),
			lists:foldl( fun(MasterNode, Started) ->
				Ping = net_adm:ping(MasterNode),
				if 
					Ping == pang ->
						error_logger:error_msg("vc_master_primary: Node ~p is not reachable(ping->pang)",[MasterNode]);
					Started == false ->
						case rpc:call(MasterNode, application, start, [vc_master]) of 
							{ok, _Pid} -> 
								error_logger:info_msg("vc_master_primary: Started secondary master at ~p", [MasterNode]),
								true;
							_  -> 
								error_logger:error_msg("vc_master_primary: Failed starting secondary master at ~p", [MasterNode]),
								false
						end;
					true ->
						true
				end
			end, false, PossibleSecMasterNodes) 
	end,
	global:whereis_name(SecondaryMasterName).

% Event .. {write,  NewRecord, ActivityId},  {delete, {Tab, key}, ActivityId}, {delete_object, OldRecord, ActivityId}
sync_secondary(State) ->
	LastSyncedId = State#state.vc_dets_last_sync_id,
	NextSyncId = State#state.vc_dets_last_sync_id + 1,
	NewLastSyncedId = case dets:lookup(vc_dets, NextSyncId) of
		[{Id,Event}] ->
			ok = gen_server:call( {global, vc_master:master_name(secondary)}, {update_table, NextSyncId, Event}),
			ok = dets:delete(vc_dets, Id),
			NextSyncId;
		_Any -> % it's already synced because after sync, it is removed
			LastSyncedId 
	end,
	State#state{vc_dets_last_sync_id = NewLastSyncedId}.


start_sync_and_monitor_of_secondary() ->
	error_logger:info_msg("vc_master_primary: initializing dets to sync mnesia updates to the secondary"),
	VcDetsFile = case application:get_env(vc_master, vc_dets_file) of 
		{ok, File} -> File;
		undefined -> "/tmp/"++atom_to_list(node())++".dets"
	end,
	file:delete(VcDetsFile), % delete existing dets if exists
	{ok, vc_dets} = dets:open_file(vc_dets, [{file,VcDetsFile}]),

	error_logger:info_msg("vc_master_primary: start 5 seconds interval synchronizer using the dets"),
	PrimaryMasterName = vc_master:master_name(primary),
	{ok, SynchronizerTRef} = timer:apply_interval(5000, gen_server, cast, [ {global,PrimaryMasterName}, sync_secondary]),

	error_logger:info_msg("vc_master_primary: start monitoring secondary master process and node"),
	SecondaryMasterPid = global:whereis_name( vc_master:master_name(secondary) ),
	SecondaryMonitorTRef = erlang:monitor(process, SecondaryMasterPid ),

	error_logger:info_msg("vc_master_primary: setting mnesia triggers for all updates of vc_interval to send messages to vc_remote_handler"),
	mnesia:subscribe({table, vc_meta, simple}),
	mnesia:subscribe({table, vc_interval, simple}),

	{SynchronizerTRef, SecondaryMonitorTRef}.

stop_sync_and_monitor_of_secondary(SynchronizerTRef) ->
	timer:cancel(SynchronizerTRef),
	mnesia:unsubscribe({table, vc_meta, simple}),
	mnesia:unsubscribe({table, vc_interval, simple}),
	ok.

	
