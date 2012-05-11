-module(vc_master_secondary).
-include("include/records.hrl").
-behaviour(gen_server).

-export([init/1, start_link/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
	primary_master, % primary master pid
	last_sync_id=0
}).

start_link() ->
	{ok,_Pid} = gen_server:start_link({global, vc_master:master_name(secondary)}, ?MODULE, [], []).

init([]) ->
	process_flag(trap_exit, true),
	catch gen_server:call({global, vc_master:master_name(primary)}, {update_secondary_status, 'initializing'}),
	Primary    = global:whereis_name( vc_master:master_name(primary) ),
	{ok, PrimaryMasterNodes} = rpc:call(erlang:node(Primary), application, get_env, [vc_master, master_nodes]),
	MasterNodes = lists:usort(PrimaryMasterNodes ++ [node()]),
	error_logger:info_msg("initializing volume cap nodes as ~p", [MasterNodes]),
	ok = application:set_env(vc_master, master_nodes, MasterNodes),
	State= init_as_secondary_master(),
	catch gen_server:call({global, vc_master:master_name(primary)}, {update_secondary_status, 'ready'}),
	erlang:monitor(process, Primary), % monitor primary, so that when it fails, this becomes the primary
	{ok, State}.

% Event .. {write,  NewRecord, ActivityId},  {delete, {Tab, key}, ActivityId}, {delete_object, OldRecord, ActivityId}
handle_call({update_table, SyncId, Event}, {Pid, _Tag},  State) when Pid == State#state.primary_master ->
	Result = mnesia:transaction( fun() ->
		case Event of
			{'write', {schema, _, _}, _ActivityId} ->
				error_logger:error_msg("vc_master_secondary: <-MP, Unexpected schema write event ~p",[Event]);
			{'delete', {schema, _}, _ActivityId} ->
				error_logger:error_msg("vc_master_secondary: <-MP, Unexpected schema delete event ~p",[Event]);
			{'write', NewRecord, _ActivityId} ->
				mnesia:write(NewRecord);
			{'delete', {Tab, Key}, _ActivityId} ->
				mnesia:delete({Tab, Key});
			{'delete_object', OldRecord, _ActivityId} ->
				mnesia:delete_object(OldRecord);
			_Any ->
				error_logger:error_msg("vc_master_secondary: <-MP, Unexpected mnesia sync event ~p",[Event])
		end,
		ok
	end),
	NewState = case Result of  
		{atomic, ok} -> 
			State#state{last_sync_id = SyncId};
		Error -> 
			error_logger:error_msg("vc_master_secondary: Mnesia sync failed with reason ~p between the primary and secondary for event ~p",[Error, Event]),
			State
	end,
	{reply, ok, NewState};

handle_call(get_state, _From, State) ->
	{reply, State, State};

handle_call(Unexpected, _From, State) ->
	error_logger:warning_msg("vc_master_secondary: unexpected handle_call message ~p", [Unexpected]),
	{reply, ignored, State}.

% from primary, need to have corresponding call
handle_cast(update_status, State) ->
	ok = gen_server:call({global, vc_master:master_name(primary)}, {update_secondary_status, 'ready'}),
	{noreply, State};

handle_cast(Unexpected, State) ->
	error_logger:warning_msg("vc_master_secondary: unexpected handle_cast message ~p", [Unexpected]),
	{noreply, State}.

handle_info(Message={'DOWN', _MonitorRef, _Type, Object, _Info}, State) -> 
	error_logger:info_msg("vc_master_seconary: DETECTED PRIMARY MASTER DOWN. ~p", [Message]),
	error_logger:info_msg("vc_master_seconary: THE SECONDARY RESTARTS AS THE PRIMARY"),
	PrimaryNode = case Object of
		Pid when is_pid(Pid) -> node(Pid);
		{_RegName,Node} -> Node
	end,
	vc_master:del_node(PrimaryNode),
	gen_server:cast(vc_master_restarter,{restart,vc_master}),
	{noreply,State};
	
handle_info(Unexpected, State) ->
	error_logger:warning_msg("vc_master_secondary: unexpected handle_info message ~p", [Unexpected]),
	{noreply, State}.

terminate(Reason, _State) ->
	error_logger:info_msg("vc_master_secondary: stopping with reason, ~p", [Reason]),
	goodbye.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%-----------------------------------------------------------------------------------------------
% internal functions for SECONDARY
%-----------------------------------------------------------------------------------------------
init_as_secondary_master() ->
	Tables = [vc_meta, vc_interval],

	error_logger:info_msg("vc_master_secondary: setting table copy of the primary if not set, and checking tables"),
	% raise exception if a table is already exists
	lists:foreach( fun(Table) ->
		case mnesia:wait_for_tables(vc_meta,1000) of
			ok -> erlang:error({"vc_master_secondary: table already exists, cannot intialize it",Table});
			_  -> ok
		end
	end, Tables),

	error_logger:info_msg("vc_master_secondary: restarting mnesia and initializing first copy of tables"),
	try 
		[_Node] = mnesia:table_info(schema,disc_copies)  % this will cause exception if schema is not created yet
	catch _:_ ->
		error_logger:info_msg("vc_master_secondary: Result mnesia:stop ~p~n", [mnesia:stop()]),
		error_logger:info_msg("vc_master_secondary: Result mnesia:delete_schema ~p~n", [mnesia:delete_schema([node()])]),
		error_logger:info_msg("vc_master_secondary: Result mnesia:create_schema ~p~n", [mnesia:create_schema([node()])]),
		error_logger:info_msg("vc_master_secondary: Result mnesia:start ~p~n", [mnesia:start()])
	end,
	PrimaryMasterName = vc_master:master_name(primary),
	PrimaryNode = node(global:whereis_name( PrimaryMasterName )),
	lists:foreach( fun(Table) ->
		catch mnesia:delete_table(Table),
		TabDef= case Table of
			vc_meta     -> [{disc_copies,[node()]},{type,set},{attributes, record_info(fields,vc_meta)}];
			vc_interval -> [{disc_copies,[node()]},{type,set},{attributes, record_info(fields,vc_interval)}]
		end,
		error_logger:info_msg("vc_master_secondary: Result mnesia:create_table ~p~n", [mnesia:create_table(Table, TabDef)]),
		WildPattern = rpc:call(PrimaryNode, mnesia, table_info, [Table, wild_pattern]),
		AllRecords = rpc:call(PrimaryNode, mnesia, dirty_match_object, [WildPattern]),
		error_logger:info_msg("vc_master_secondary: writing ~p~n", [AllRecords]),
		mnesia:transaction(fun() ->
			lists:foreach( fun(Record) ->
				mnesia:write(Record) 
			end, AllRecords)
		end) 
	end, Tables),

	#state{
		primary_master = global:whereis_name(PrimaryMasterName)
	}.

