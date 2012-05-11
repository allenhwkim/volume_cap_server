-module(vc_meta_handler).
-include("include/records.hrl").
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {dc_id} ).

%-----------------------------------------------------------------------------------------------
% gen_server functions
%-----------------------------------------------------------------------------------------------
start_link() ->
	{ok,_Pid} = gen_server:start_link({global, vc_master:handler_name(meta)}, ?MODULE, [], []).

init([]) ->
	process_flag(trap_exit, true),
	ok = vc_master:wait_for_primary_vc_master_ready(0),
	{ok, DcId} = application:get_env(vc_master, dc_id),
	{ok, #state{dc_id=DcId}}.

handle_call({'update', VcMeta=#vc_meta{total=Total}}, _From, State)  ->
	handle_call({'update', VcMeta, Total}, _From, State);
handle_call({'update', VcMeta=#vc_meta{}, InitialTotal}, _From, State)  ->
	error_logger:info_msg("vc_meta_handler: received update message ~500p",[VcMeta]),
	IntervalActions = get_meta_to_interval_actions(VcMeta, InitialTotal),
	mnesia:dirty_write(VcMeta),
	IntervalHandler = vc_master:handler_name(interval),
	lists:foreach( fun(IntervalAction) ->
		if 
			IntervalAction == 'update_initial_interval_only' ->
				gen_server:cast({global, IntervalHandler}, {IntervalAction, VcMeta#vc_meta{total=InitialTotal}});  
			true ->
				gen_server:cast({global, IntervalHandler}, {IntervalAction, VcMeta})
		end
	end,IntervalActions),
	{reply, ok, State};

handle_call({'delete', VcMetaId}, _From, State) ->
	error_logger:info_msg("vc_meta_handler: received delete message ~500p",[VcMetaId]),
	catch  mnesia:dirty_delete(vc_meta, VcMetaId),
	gen_server:cast({global, vc_master:handler_name(interval)}, {'delete_all_intervals', VcMetaId}), 
	{reply, ok, State};

handle_call(_Any, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Any, State) -> {noreply, State}.
handle_info(_Any, State) -> {noreply, State}.
terminate(_OtherReason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

get_meta_to_interval_actions(VcMeta, InitialTotal) ->
	#vc_meta{id=NewId,total=NewTotal,start=NewStart,'end'=NewEnd} = VcMeta,
	{{ThisYear,ThisMonth,ThisDate},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{ThisYear, ThisMonth, ThisDate}, {0,0,0}}),
	ExistingMeta = mnesia:dirty_read(vc_meta, NewId),
	% intervals are created ahead
	ExistingMetaActive = case ExistingMeta of 
		[Old] when Old#vc_meta.start =< Today, Old#vc_meta.'end' >Today-> 
			'true';
		_Any -> 
			'false'
	end,
	NewMetaActive = if 
		NewStart =< Today, NewEnd > Today -> 
			'true';
		true -> 
			'false'
	end,
	IntervalCommands = if 
		ExistingMeta == [VcMeta] -> % the same meta, nothing changed
			error_logger:info_msg("vc_meta_handler: no change in meta ~500p",[VcMeta]),
			['do_nothing'];
		ExistingMetaActive==true, NewMetaActive == false ->
			['delete_all_intervals'];
		ExistingMetaActive==false, NewMetaActive == true, NewTotal /= InitialTotal ->
			['delete_non_active_intervals', 'create_new_intervals', 'update_initial_interval_only'];
		ExistingMetaActive==false, NewMetaActive == true ->
			['delete_non_active_intervals', 'create_new_intervals'];
		ExistingMetaActive==true, NewMetaActive == true ->
			['update_active_intervals'];
		ExistingMetaActive==false, NewMetaActive == false ->
			['do_nothing']
	end,
	IntervalCommands.
