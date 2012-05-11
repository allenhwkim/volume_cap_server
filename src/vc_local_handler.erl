-module(vc_local_handler).
-include("include/records.hrl").
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-define(STATS, [{topup,0},{return,0},{b_new,0},{b_update,0},{b_delete,0}] ).
-record(state, {server_nodes=[],stats=?STATS} ).

start_link() ->
	{ok,_Pid} = gen_server:start_link({global, vc_master:handler_name(local)}, ?MODULE, [], []).

init([]) ->
	process_flag(trap_exit, true),
	ok = vc_master:wait_for_primary_vc_master_ready(0),
	State = #state{},
	{ok, State}.

handle_call('get_state', _From, State) -> 
	{reply, State, State};

handle_call('reset_stats', _From, State)  ->
	NewState = State#state{stats=?STATS},
	{reply, NewState, NewState};
handle_call('join', _From={Pid,_Tag}, State) -> % from VS
	error_logger:info_msg("vc_local_handler: <-VS(~p), Received join message",[node(Pid)]),
	AllNodes = lists:usort(State#state.server_nodes ++ [node(Pid)]),
	{reply, AllNodes, State#state{server_nodes=AllNodes}};

handle_call('current_intervals', _From={Pid,_Tag}, State) -> % from VS
	error_logger:info_msg("vc_local_handler: <-VS(~p), Received current_intervals message",[node(Pid)]),
	AllVcMetaKeys = mnesia:dirty_all_keys(vc_meta),
	AllVcIntervals = lists:foldl( fun(MetaKey, Result) ->
		[VcMeta] = mnesia:dirty_read(vc_meta, MetaKey), 
		case vc_interval_handler:current_interval(VcMeta) of
			[IntervalStart, _IntervalEnd] -> 
				[VcInterval] = mnesia:dirty_read(vc_interval, vc_interval_handler:vc_interval_key(VcMeta#vc_meta.id, IntervalStart)),
				Result ++ [VcInterval];
			{error, invalid_start_end} ->
				Result
		end
	end, [], AllVcMetaKeys),
	{reply, AllVcIntervals, State};

handle_call(Unexpected, From, State)  ->
	error_logger:info_msg("vc_local_handler: Unexpected handle_call message ~p from ~p",[Unexpected, From]),
	{reply, ignore, State}.

handle_cast({'broadcast', VcInterval, Operation}, State) ->  % from IH
	NewState = case Operation of
		'new' ->  % spread the news, so that they can call the action
			lists:foreach( fun(Node) ->
				error_logger:info_msg("vc_local_handler: ->VS(~p), sending broadcast msg ~p of ~500p",[Node, Operation, VcInterval]),
				gen_server:cast({volumecap_server,Node}, {'broadcast', VcInterval, Operation})	
			end, State#state.server_nodes),
			add_stats('b_new',State);
		'update' ->  % total decreased more than available, spread the news to stop serving it
			lists:foreach( fun(Node) ->
				error_logger:info_msg("vc_local_handler: ->VS(~p), sending broadcast msg ~p of ~500p",[Node, Operation, VcInterval]),
				gen_server:cast({volumecap_server,Node}, {'broadcast', VcInterval, Operation})	
			end, State#state.server_nodes),
			add_stats('b_update',State);
		'delete' ->  % spread the news, so that they can delete it
			lists:foreach( fun(Node) ->
				error_logger:info_msg("vc_local_handler: ->VS(~p), sending broadcast msg ~p of ~500p",[Node, Operation, VcInterval]),
				gen_server:cast({volumecap_server,Node}, {'broadcast', VcInterval, Operation})	
			end, State#state.server_nodes),
			add_stats('b_delete',State)
	end,
	{noreply, NewState};

handle_cast({'topup',IntervalId,Amount,From}, State) when is_pid(From) -> % from VS
	handle_cast({'topup',IntervalId,Amount,node(From)}, State);
handle_cast({'topup',IntervalId,Amount,NodeFrom}, State) -> % from VS
	error_logger:info_msg("vc_local_handler: <- VS(~p) received topup request of ~500p, amount ~p",[NodeFrom, IntervalId, Amount]),
	case mnesia:dirty_read(vc_interval, IntervalId) of
		[] -> ok; % this must not happen.
		[VcInterval] ->
			#vc_interval{total=IntTotal,available=Available,consumed=Consumed} = VcInterval,
			ThresholdAmount = threshold_amount(IntTotal),
			if
				Available >= Amount ->
					NewVcInterval = VcInterval#vc_interval{
						available = NewIntAvailable = Available - Amount,
						consumed = Consumed + Amount},
					mnesia:dirty_write(NewVcInterval),
					gen_server:cast({volumecap_server, NodeFrom}, {'topup',VcInterval, Amount}),	
					if
						NewIntAvailable =< ThresholdAmount ->
							error_logger:info_msg("vc_local_handler: ->RH, sending topup request, available(~p) =< threshold(~p)",[NewIntAvailable, ThresholdAmount]), 
							gen_server:cast({global,vc_master:handler_name(remote)}, {'topup',NewVcInterval, Amount});	
						true ->
							ok
					end;
				true ->
					ok
			end
	end,
	{noreply, add_stats('topup',State)};

handle_cast({'return',{IntId,ReturnAmount},From}, State) -> % from VS
	%error_logger:info_msg("vc_local_handler: <-VS(~p), received return message ~500p",[node(From), {IntId,ReturnAmount}]),
	error_logger:info_msg("vc_local_handler: <-VS(~p), RECEIVED RETURN MESSAGE ~500p",[node(From), {IntId,ReturnAmount}]),
	[VcInterval] = mnesia:dirty_read(vc_interval,IntId),
	#vc_interval{available=Available, consumed=Consumed} = VcInterval,
	NewVcInterval = VcInterval#vc_interval{available = Available+ReturnAmount, consumed=Consumed-ReturnAmount},
	mnesia:dirty_write(NewVcInterval),
	error_logger:info_msg("vc_local_handler: updated ~500p",[NewVcInterval]),
	{noreply, State};

handle_cast({'return',ReturnItems,From}, State) -> % from VS
	%error_logger:info_msg("vc_local_handler: <-VS(~p), received ~p return message(s)",[node(From), length(ReturnItems)]),
	error_logger:info_msg("vc_local_handler: <-VS(~p), RECEIVED ~p RETURN MESSAGE(S)",[node(From), length(ReturnItems)]),
	lists:foreach( fun({VcIntId,ReturnAmount}) ->
		case mnesia:dirty_read(vc_interval,VcIntId) of
			[] -> ok; % this must not happen
			[VcInterval] ->
				#vc_interval{available=Available, consumed=Consumed} = VcInterval,
				NewVcInterval = VcInterval#vc_interval{available = Available+ReturnAmount, consumed=Consumed-ReturnAmount},
				mnesia:dirty_write(NewVcInterval),
				error_logger:info_msg("vc_local_handler: updated ~500p",[NewVcInterval])
		end
	end, ReturnItems),
	NewAllNodes = State#state.server_nodes -- [node(From)],
	error_logger:info_msg("vc_local_handler: removed ~p from server_nodes, resulting ~p",[node(From),NewAllNodes]),
	{noreply, add_stats('return',State#state{server_nodes=NewAllNodes})};

handle_cast(Unexpected, State) -> 
	error_logger:info_msg("vc_local_handler: Unexpected handle_cast message ~p",[Unexpected]),
	{noreply, State}.
	
handle_info(Unexpected, State) ->
	error_logger:info_msg("vc_local_handler: Unexpected handle_info message ~p",[Unexpected]),
	{noreply, State}.

terminate(_OtherReason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

threshold_amount(DcTotal) ->
	ThresholdPct = case application:get_env(vc_master, threshold_pct) of
		{ok, Pct} ->Pct;
		undefined ->10
	end,
	erlang:trunc(DcTotal * (ThresholdPct/100) * 100) / 100.

add_stats(Key,State=#state{stats=Stats}) ->
	NewStats = lists:map( fun({K,V}) ->
		if 
			K == Key -> {K,V+1};
			true     -> {K,V}
		end
	end, Stats),
	%error_logger:info_msg("vc_local_handler: Stats ~500p",[NewStats]),
	State#state{stats=NewStats}.
