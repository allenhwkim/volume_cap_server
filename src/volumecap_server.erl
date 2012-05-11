-module(volumecap_server).
-include("include/records.hrl").
-behaviour(gen_server).
-export([start/0, stop/0, status/0, join/0]).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-define(STATS, [{lookup,0},{deduct,0},{b_new,0},{b_update,0},{b_delete,0},{topup,0}] ).
-record(state, {local_handler,stats=?STATS} ).

start() ->
	gen_server:start({local, ?MODULE}, ?MODULE, [], []).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
	gen_server:cast(?MODULE,'stop').

status() ->
	gen_server:call(?MODULE, 'get_state').

join() ->
	gen_server:call(?MODULE, 'join').

init([]) ->
	process_flag(trap_exit, true),
	WaitFun = fun() -> 
		_AllNodes = gen_server:call({global,vc_master:handler_name(local)},'join')
	end,
	error_logger:info_msg("volumecap_server: ->LH, sending join message"),
	wait_until(WaitFun,5),
	set_default_env_variables(),
	create_volumecap_if_not_exists(),
	LocalHandler = global:whereis_name(vc_master:handler_name(local)),
	error_logger:info_msg("volumecap_server: ->LH, sending request of all of current intervals"),
	CurrentIntervals = gen_server:call(LocalHandler, 'current_intervals'),
	lists:foreach( fun(VcInterval) ->
		#vc_interval{'id'=IntId,'total'=IntTotal} = VcInterval,
		case mnesia:dirty_read(volumecap, IntId) of
			[Volumecap] when Volumecap#volumecap.available > 0 ->
				ok;
			_Any -> %not found or found with 0 available
				TopupAmount = topup_amount(IntTotal),
				error_logger:info_msg("volumecap_server: ->LH, sening initial topup request id:~p amount: ~p",[IntId,TopupAmount]),
				gen_server:cast(LocalHandler, {'topup',IntId,TopupAmount,self()})
		end
	end, CurrentIntervals),
	{ok, #state{local_handler=LocalHandler}}.

handle_call('join', _From, State)  ->
	AllNodes = gen_server:call({global,vc_master:handler_name(local)},'join'),
	LocalHandler = global:whereis_name(vc_master:handler_name(local)),
	{reply, AllNodes, State#state{local_handler=LocalHandler}};

handle_call('get_state', _From, State)  ->
	{reply, State, State};

handle_call('reset_stats', _From, State)  ->
	NewState = State#state{stats=?STATS},
	{reply, NewState, NewState};

handle_call({Command,VcMetaId}, _From, State)  when Command=='lookup'; Command=='query' ->
	Volumecaps = mnesia:dirty_index_read(volumecap, VcMetaId, #volumecap.meta_id),
	Result = case current_volumecap(Volumecaps) of
		'undefined' ->
			{error, not_found};
		CurrentVolumecap ->
			{ok, CurrentVolumecap#volumecap.available}
	end,
	{reply, Result, add_stats('lookup',State)};

handle_call({Command,VcMetaId, Amount}, _From, State) when Command=='deduct'; Command=='consume' ->
	Volumecaps = mnesia:dirty_index_read(volumecap, VcMetaId, #volumecap.meta_id),
	Result = case current_volumecap(Volumecaps) of
		'undefined' ->
			{error, not_found};
		CurrentVolumecap ->
			#volumecap{'id'=VcId,'total'=VcTotal,'available'=VcAvailable,'consumed'=VcConsumed} = CurrentVolumecap,
			ThresholdAmount = threshold_amount(VcTotal), %fyi, Vctotal=d.c. total
			if	 % if close to bottom, need to send topup request
				(VcAvailable - Amount) >= 0, (VcAvailable - Amount) =< ThresholdAmount -> 
					TopupAmount = topup_amount(VcTotal),
					error_logger:info_msg("volumecap_server: ->LH, sending topup request id:~p amount: ~p",[VcId,TopupAmount]),
					gen_server:cast(State#state.local_handler, {'topup',VcId,TopupAmount,self()});
				true ->
					ok
			end,
			if 	
				(VcAvailable - Amount) < 0 ->
					{error,capped}; 
				true -> % enough amount left
					mnesia:dirty_write( CurrentVolumecap#volumecap{
						'available'=(VcAvailable - Amount),
						'consumed'=(VcConsumed + Amount) } ),
					{ok, (VcAvailable - Amount)}
			end
	end,
	{reply, Result, add_stats('deduct', State)};

handle_call(Unexpected, From, State)  ->
	error_logger:info_msg("vc_local_handler: Unexpected handle_call message ~p from ~p",[Unexpected, From]),
	{reply, ignore, State}.

handle_cast({'broadcast', VcInterval, Operation}, State) ->  % from LH
	#vc_interval{id=IntervalId,start=IntervalStart,total=NewTotal,available=IntervalAvailable} = VcInterval,
	error_logger:info_msg("volumecap_server: <-LH, received broadcast message ~p ~500p",[Operation, VcInterval]),
	TopupAmount = topup_amount(NewTotal),
	CurrentTime = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
	NewState = case Operation of
		'new' -> 
			error_logger:info_msg("volumecap_server: ->LH, responding to new interval, sening topup request id:~500p amount: ~p",[IntervalId,TopupAmount]),
			gen_server:cast(State#state.local_handler, {'topup',IntervalId, TopupAmount,self()}),
			add_stats('b_new',State);
		'update' when IntervalAvailable == 0 -> % decreased more than available, need to stop serving it
			case mnesia:dirty_read(volumecap, IntervalId) of
				[] -> ok;
				[Volumecap] when IntervalStart < CurrentTime -> % current interval
					mnesia:dirty_write(Volumecap#volumecap{total=NewTotal,available=0});
				[#volumecap{available=Available}=Volumecap] when IntervalStart >= CurrentTime -> % next interval, need to return extras
					error_logger:info_msg("volumecap_server: ->LH, RESPONDING TO UPDATE, SENDING 'RETURN' MESSAGE ~500p",[{IntervalId, Available-TopupAmount}]),
					gen_server:cast(State#state.local_handler, {'return', {IntervalId,(Available-TopupAmount)}, self()}),
					mnesia:dirty_write(Volumecap#volumecap{total=NewTotal,available=TopupAmount})
			end,
			add_stats('b_update',State);
		'update' when IntervalAvailable > 0 ->  % increase or decrease
			case mnesia:dirty_read(volumecap, IntervalId) of
				[] -> ok;
				[#volumecap{id=VcId,total=PrevTotal,available=Available}=Volumecap] ->
					if 
						NewTotal > PrevTotal, TopupAmount > Available -> % increases, and less available, 'topup' required 
							error_logger:info_msg("volumecap_server: ->LH, responding to update, sending topup request id: ~500p amount: ~p",[VcId,TopupAmount-Available]),
							gen_server:cast(State#state.local_handler, {'topup',VcId, (TopupAmount-Available),self()}),
							mnesia:dirty_write(Volumecap#volumecap{total=NewTotal});
						NewTotal < PrevTotal, Available > TopupAmount ->  %decreases, and more available, 'return' required
							error_logger:info_msg("volumecap_server: ->LH, RESPONDING TO UPDATE, SENDING 'RETURN' MESSAGE ~500p",[{VcId, Available-TopupAmount}]),
							gen_server:cast(State#state.local_handler, {'return', {VcId,(Available-TopupAmount)}, self()}),
							mnesia:dirty_write(Volumecap#volumecap{total=NewTotal,available=TopupAmount});
						true -> % same total, do nothing (this is filtered at meta handler)
							ok
					end
			end,
			add_stats('b_update',State);
		'delete' ->  
			case mnesia:dirty_read(volumecap, VcInterval#vc_interval.id) of
				[] -> ok;
				_Any -> 
					mnesia:dirty_delete(volumecap, VcInterval#vc_interval.id),
					error_logger:info_msg("volumecap_server: removed volumecap ~p",[IntervalId])
			end,
			add_stats('b_delete',State)
	end,
	{noreply, NewState};

handle_cast({'topup',VcInterval,Amount}, State) -> % topup response from LH
	#vc_interval{id=IntervalId} = VcInterval,
	error_logger:info_msg("volumecap_server: <-LH, received topup response of Id ~p Amount ~p",[IntervalId,Amount]),
	#vc_interval{id=IntervalId,meta_id=MetaId,start=Start,'end'=End,interval=Interval,total=Total} = VcInterval,
	NewVolumecap = case mnesia:dirty_read(volumecap, IntervalId) of
		[] ->  % new one
			#volumecap{
				'id'=IntervalId,
				'meta_id' = MetaId,
				'start'=Start,
				'end'=End,
				'interval'=Interval,
				'total'=Total,
				'available'=Amount,
				'consumed'=0 };
		[#volumecap{available=Available}=Volumecap] -> 
			Volumecap#volumecap{total=Total,available=Available+Amount}
	end,
	mnesia:dirty_write(NewVolumecap),
	error_logger:info_msg("volumecap_server: updated volumecap ~500p",[NewVolumecap]),
	{noreply, add_stats('topup',State)};

handle_cast('stop',  State) ->
	error_logger:info_msg("volumecap_server: stopping by gen_server:cast"),
	{stop, normal, State};

handle_cast(Unexpected, State) -> 
	error_logger:info_msg("vc_local_handler: Unexpected handle_cast message ~p",[Unexpected]),
	{noreply, State}.

handle_info(Unexpected, State) ->
	error_logger:info_msg("vc_local_handler: Unexpected handle_info message ~p",[Unexpected]),
	{noreply, State}.

terminate(Reason, State) -> 
	error_logger:info_msg("volumecap_server: ->LH, SHUTTING DOWN WITH REASON ~p, RETURNING ALL AVAILABLE VOLUMECAPS",[Reason]),
	{atomic, VolumecapAvailables} = mnesia:transaction(fun() ->
		mnesia:foldl(fun(#volumecap{id=Id,available=Available}, Acc) ->
			if 
				Available > 0 -> Acc ++ [{Id,Available}];
				true          -> Acc
			end
		end,[],volumecap)
	end), 
	{atomic,ok} = mnesia:clear_table(volumecap),
	gen_server:cast(State#state.local_handler, {'return', VolumecapAvailables, self()}),
	goodbye.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

wait_until(Fun, MaxSec) ->
	wait_until(Fun, MaxSec, 0).
wait_until(Fun, MaxSec, Sec) when MaxSec =< Sec ->
	io:format(user, "waiting for vc_local_handler get ready ~p~n",[Fun]),
	error(timeout);
wait_until(Fun, MaxSec, Sec) when MaxSec > Sec ->
	try
		erlang:apply(Fun,[])
	catch _Class:_Error ->
		timer:sleep(1000),
		wait_until(Fun, MaxSec,Sec+1)
	end.

topup_amount(DcTotal) -> % DcTotal is total amount assigned to this data center
    % for example, for 100 volume cap with two data center and topup % is 10%, then topup amount is 5=100/2/10%
	% 6.66 = trunc(33.3333 * 0.2 * 100)/100.%
	% 10.0 = trunc(50 * 0.2 * 100)/100.  
	{ok, TopupPct} = application:get_env(volumecap_server, topup_pct),
	erlang:trunc(DcTotal * (TopupPct/100) * 100)/100.

threshold_amount(DcTotal) ->
	{ok, ThresholdPct} = application:get_env(volumecap_server, threshold_pct),
	TopupAmount = topup_amount(DcTotal),
	erlang:trunc(TopupAmount * (ThresholdPct/100) * 100) / 100.

current_volumecap(Volumecaps) ->
	CurrentTime = calendar:datetime_to_gregorian_seconds(calendar:local_time()), 
	CurrentVolumecap = lists:foldl( fun(Volumecap=#volumecap{'start'=Start,'end'=End}, Result) ->
		case Result of
			'undefined' when (Start =< CurrentTime) and (End > CurrentTime) -> 
				Volumecap;
			AlreadyFound when is_record(AlreadyFound, volumecap) -> 
				AlreadyFound;
			_Any -> 'undefined'
		end
	end, 'undefined', Volumecaps),
	CurrentVolumecap.

create_volumecap_if_not_exists() ->
	try 
		[_Node] = mnesia:table_info(schema,disc_copies)  % this will cause exception if schema is not created yet
	catch _:_ ->
		error_logger:info_msg("volumecap_server: Result mnesia:stop ~p~n", [mnesia:stop()]),
		error_logger:info_msg("volumecap_server: Result mnesia:delete_schema ~p~n", [mnesia:delete_schema([node()])]),
		error_logger:info_msg("volumecap_server: Result mnesia:create_schema ~p~n", [mnesia:create_schema([node()])]),
		error_logger:info_msg("volumecap_server: Result mnesia:start ~p~n", [mnesia:start()])
	end,
	try
		mnesia:table_info(volumecap, disc_copies)  % this will cause exception if table not exists
	catch _:_ ->
		CreateTableResult = mnesia:create_table(volumecap, [
				{disc_copies,[node()]},
				{type,set},
				{attributes, record_info(fields,volumecap)},
				{index,[meta_id]}]),
		error_logger:info_msg("volumecap_server: Creating volumecap table ~p", [CreateTableResult])
	end.

set_default_env_variables()->
	case application:get_env(volumecap_server, topup_pct) of
		'undefined' -> application:set_env(volumecap_server, topup_pct, 10);
		_ -> ok
	end,
	case application:get_env(volumecap_server, threshold_pct) of
		'undefined' -> application:set_env(volumecap_server, threshold_pct, 10);
		_ -> ok
	end,
	ok.

add_stats(Key,State=#state{stats=Stats}) ->
	NewStats = lists:map( fun({K,V}) ->
		if 
			K == Key -> {K,V+1};
			true     -> {K,V}
		end
	end, Stats),
	%error_logger:info_msg("volumecap_server: Stats ~500p",[NewStats]),
	State#state{stats=NewStats}.
