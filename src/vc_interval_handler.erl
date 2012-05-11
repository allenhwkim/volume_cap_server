-module(vc_interval_handler).
-include("include/records.hrl").
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {stats} ).
-compile(export_all).
-define(STATS, [{del_all,0},{del_some,0},{add_new,0},{updt_active,0},{updt_initial,0},{do_nothing,0},{spawn_intervals,0}]).

start_link() ->
	{ok,_Pid} = gen_server:start_link({global, vc_master:handler_name(interval)}, ?MODULE, [], []).

init([]) ->
	process_flag(trap_exit, true),
	ok = vc_master:wait_for_primary_vc_master_ready(0),
	ok = create_missing_intervals(),
	{ok, _TimerRef} = timer:apply_interval(1000*60*5, gen_server, cast, [self(), spawn_intervals]),
	{ok, #state{stats=?STATS}}.

handle_call('get_state', _From, State)  ->
	{reply, State, State};

handle_call('reset_stats', _From, State)  ->
	NewState = State#state{stats=?STATS},
	{reply, NewState, NewState};

handle_call(Unexpected, From, State)  ->
	error_logger:info_msg("vc_interval_handler: Unexpected handle_call message ~p from ~p",[Unexpected, From]),
	{reply, ignore, State}.

handle_cast({'delete_all_intervals', VcMetaId}, State) -> % from MH
	error_logger:info_msg("vc_interval_handler: <-MH received 'delete_all_intervals' of ~p",[VcMetaId]),
	LocalHandler = {global, vc_master:handler_name(local)},
	lists:foreach( fun(VcInterval) ->
		mnesia:dirty_delete_object(VcInterval),
		gen_server:cast(LocalHandler, {'broadcast', VcInterval, 'delete'})
	end, all_intervals(VcMetaId)),
	NewState = add_stats('del_all',State),
	{noreply, NewState};

handle_cast({MetaToIntervalAction, VcMeta=#vc_meta{}}, State) -> % from MH
	error_logger:info_msg("vc_interval_handler: <-MH received ~p of ~500p",[MetaToIntervalAction, VcMeta]),
	LocalHandler = {global, vc_master:handler_name(local)},
	NewState = case MetaToIntervalAction of
		'delete_non_active_intervals' ->
			lists:foreach( fun(VcInterval) ->
				mnesia:dirty_delete_object(VcInterval),
				error_logger:info_msg("vc_interval_handler: ->LH sending delete of ~500p",[VcInterval]),
				gen_server:cast(LocalHandler, {'broadcast', VcInterval, 'delete'})
			end, non_active_intervals(VcMeta)),
			add_stats('del_some',State);
		'create_new_intervals' ->
			lists:foreach( fun(VcInterval) ->
				mnesia:dirty_write(VcInterval),
				error_logger:info_msg("vc_interval_handler: ->LH sending create of ~500p",[VcInterval]),
				gen_server:cast(LocalHandler, {'broadcast', VcInterval, 'new'})	
			end, new_intervals(VcMeta)),
			add_stats('add_new',State);
		'update_active_intervals' ->
			lists:foreach( fun(VcInterval) ->
				mnesia:dirty_write(VcInterval),
				error_logger:info_msg("vc_interval_handler: ->LH sending update of ~500p",[VcInterval]),
				gen_server:cast(LocalHandler, {'broadcast', VcInterval, 'update'})
			end, updating_intervals(VcMeta)),
			add_stats('updt_active',State);
		'update_initial_interval_only' ->  % update only the active initial interval
			VcInterval = updating_initial_interval(VcMeta),  
			mnesia:dirty_write(VcInterval),
			error_logger:info_msg("vc_interval_handler: ->LH sending update of ~500p",[VcInterval]),
			gen_server:cast(LocalHandler, {'broadcast', VcInterval, 'update'}),
			add_stats('updt_initial',State);
		'do_nothing' ->
			add_stats('do_nothing',State)
	end,
	{noreply, NewState};
		
handle_cast({IntervalAction, {_,MetaId,IntStart}=IntervalId}, State) ->  % this happens manually
	error_logger:info_msg("vc_interval_handler: received interval command ~500p",[{IntervalAction, IntervalId}]),
	LocalHandler = {global, vc_master:handler_name(local)},
	case IntervalAction of
		'create' ->
			try
				[VcMeta] = mnesia:dirty_read(vc_meta, MetaId),
				[CurrentInterval,NextInterval] = new_intervals(VcMeta),
				VcInterval = if 
					IntStart == CurrentInterval#vc_interval.start -> CurrentInterval;
					IntStart == NextInterval#vc_interval.start ->    NextInterval;
					true -> error("Invalid interval start time")
				end,
				mnesia:dirty_write(VcInterval),
				error_logger:info_msg("vc_interval_handler: ->LH sending create of ~500p",[VcInterval]),
				gen_server:cast({global, vc_master:handler_name(local)}, {'broadcast', VcInterval, 'new'})  
			catch _:Error ->
				error_logger:error_msg("vc_interval_handler: error ~p on create ~p",[Error, IntervalId])
			end;
		'delete' ->
			try
				[VcInterval] = mnesia:dirty_read(vc_interval, IntervalId),
				mnesia:dirty_delete_object(VcInterval),
				error_logger:info_msg("vc_interval_handler: ->LH sending delete of ~500p",[VcInterval]),
				gen_server:cast(LocalHandler, {'broadcast', VcInterval, 'delete'})
			catch _:Error ->
				error_logger:error_msg("vc_interval_handler: error ~p on delete ~p",[Error, IntervalId])
			end
	end,
	{noreply, State};

handle_cast('spawn_intervals', State) -> 
	{{_Y,_M,_D},{H,M,_S}} = calendar:local_time(),
	MatchNum = (H rem 12)*12 + (erlang:trunc(M/5)), % 00:05-> 1, 00:10->2, ... 01:00->12, 23:56 -> 143
	handle_cast({'spawn_intervals',MatchNum}, State);

% good for test to separate like this
handle_cast({'spawn_intervals',MatchNum}, State) -> % from timer, or manually
	VcIntervals = spawn_intervals(MatchNum),
	error_logger:info_msg("vc_interval_handler: spawning new(~p) vc_intervals ~500p",[MatchNum,VcIntervals]),
	lists:foreach( fun(VcInterval) ->
		mnesia:dirty_write(VcInterval),
		gen_server:cast({global, vc_master:handler_name(local)}, {'broadcast', VcInterval, 'new'})  
	end, VcIntervals),
	{noreply, add_stats('spawn_intervals',State)};
	
handle_cast(Unexpected, State) -> 
	error_logger:info_msg("vc_interval_handler: Unexpected handle_cast message ~p",[Unexpected]),
	{noreply, State}.

handle_info(Unexpected, State) ->
	error_logger:info_msg("vc_interval_handler: Unexpected handle_info message ~p",[Unexpected]),
	{noreply, State}.

terminate(_OtherReason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

% returns the current interval start and end
current_interval(#vc_meta{interval=Interval, start=MetaStart, 'end'=MetaEnd}) ->
	current_interval(Interval, MetaStart, MetaEnd).
current_interval(Interval, MetaStart, MetaEnd) ->
	{{ThisYear,ThisMonth,ThisDate},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{ThisYear, ThisMonth, ThisDate}, {0,0,0}}), %% 00:00:00 today
	if 
		MetaStart =< Today, MetaEnd > Today ->
			case Interval of 
				single ->
					[MetaStart, MetaEnd]; 
				day ->
					[Today, Today+86400-1];
				week ->
					DayOfTheWeek = calendar:day_of_the_week({ThisYear, ThisMonth, ThisDate}),
					ThisWeekBegins = calendar:datetime_to_gregorian_seconds({{ThisYear, ThisMonth, ThisDate}, {0,0,0}}) - ((DayOfTheWeek-1)*86400), %% 00:00:00 previous Monday
					ThisWeekEnds = ThisWeekBegins + (7*86400)-1, %% 23:59:59 coming Sunday
					[ThisWeekBegins, ThisWeekEnds];
				month ->
					LastDayOfMonth = calendar:last_day_of_the_month(ThisYear, ThisMonth),
					ThisMonthBegins = calendar:datetime_to_gregorian_seconds({{ThisYear, ThisMonth, 1}, {0,0,0}}), %% 00:00:00 first of month
					ThisMonthEnds = ThisMonthBegins + (LastDayOfMonth*86400)-1, %% 23:59:59 last day of month
					[ThisMonthBegins, ThisMonthEnds] 
			end;
		true ->
			{error, invalid_start_end}
	end.

% returns the next interval start and end
next_interval(#vc_interval{interval=Interval, start=IntervalStart, 'end'=IntervalEnd}) ->
	next_interval(Interval, IntervalStart, IntervalEnd).
next_interval(Interval, IntervalStart, IntervalEnd) ->
	{{ThisYear,ThisMonth,ThisDate},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{ThisYear, ThisMonth, ThisDate}, {0,0,0}}), %% 00:00:00 today
	if 
		IntervalStart =< Today, IntervalEnd > Today ->
			case Interval of 
				day ->
					[IntervalEnd+1, IntervalEnd+86400];
				week ->
					[IntervalEnd+1, IntervalEnd+(7*86400)];
				month ->
					NextMonthStarts = IntervalEnd +1,
					{{EY,EM,1},{0,0,0}} = calendar:gregorian_seconds_to_datetime(NextMonthStarts), % next month start
					LastDayOfMonth = calendar:last_day_of_the_month(EY, EM),
					NextMonthEnds = NextMonthStarts + (LastDayOfMonth*86400)-1, %% 23:59:59 last day of month
					[IntervalEnd+1, NextMonthEnds] 
			end;
		true ->
			{error, invalid_start_end}
	end.

spawn_intervals(MatchNum) ->
	MetaIds = mnesia:dirty_all_keys(vc_meta),
	CurrentTime = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
	VcIntervals = lists:foldl( fun(MetaId, Result) ->
		if
			(MetaId rem 144) == MatchNum ->  % 1 out of 144 (12 hours / 5 mins)
				[VcMeta] = mnesia:dirty_read(vc_meta, MetaId),
				if 
					VcMeta#vc_meta.start < CurrentTime, VcMeta#vc_meta.'end' > CurrentTime ->
						[CurrentInterval,NextInterval] = new_intervals(VcMeta),
						case mnesia:dirty_read(vc_interval, CurrentInterval#vc_interval.id) of
							[] ->
								error_logger:error_msg("vc_interval_handler: ERROR. ~p NOT FOUND. THIS MUST NOT HAPPEN",[CurrentInterval]),
								mnesia:dirty_write(CurrentInterval), 
								gen_server:cast({global, vc_master:handler_name(local)}, {'broadcast',CurrentInterval,'new'});
							[_CurrentInterval] -> 
								ok
						end,
						case mnesia:dirty_read(vc_interval, NextInterval#vc_interval.id) of
							[_NextInterval] ->
								Result;
							[] -> 
								Result ++ [NextInterval]
						end;
					true -> % expired, or future meta
						Result
				end;
			true ->  % non-matching number, 143 out of 144
				Result
		end
	end, [], MetaIds),
	VcIntervals.

all_intervals(VcMetaId) ->
	mnesia:dirty_index_read(vc_interval,VcMetaId,#vc_interval.meta_id).

non_active_intervals(VcMeta) ->
	CurrentTime = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
	lists:filter(fun(VcInterval) ->
		(VcInterval#vc_interval.start >= CurrentTime) or (VcInterval#vc_interval.'end' < CurrentTime) 
	end, all_intervals(VcMeta)).

new_intervals(VcMeta=#vc_meta{id=MetaId,total=MetaTotal,interval=MetaInterval}) ->
	[CurrentStart,CurrentEnd] = vc_interval_handler:current_interval(VcMeta), 
	[NextStart, NextEnd]      = vc_interval_handler:next_interval(MetaInterval, CurrentStart, CurrentEnd), 
	{ok, DcId} = application:get_env(vc_master, dc_id), 
	{ok, DcIds} = application:get_env(vc_master, dc_ids), 
	DcTotal = round( MetaTotal/length(DcIds)*100)/100, % 100 -> 100.00, 33.3333333 -> 33.33
	CurrentInterval = #vc_interval{
		'id'={DcId,MetaId,CurrentStart},
		'dc_id'=DcId,
		'meta_id'=MetaId, 
		'start'=CurrentStart, 
		'end'=CurrentEnd, 
		'interval'=MetaInterval, 
		'total'=DcTotal,
		'available'=DcTotal,
		'consumed'=0},
	NextInterval = CurrentInterval#vc_interval{
		'id'={DcId,MetaId,NextStart},
		'start'=NextStart, 
		'end'=NextEnd},
	[CurrentInterval, NextInterval].

updating_intervals(VcMeta) ->
	#vc_meta{id=MetaId,interval=MetaInterval,total=MetaTotal} = VcMeta,
	{ok, DcId} = application:get_env(vc_master, dc_id), 
	{ok, DcIds} = application:get_env(vc_master, dc_ids), 
	NewTotal = round( MetaTotal/length(DcIds)*100)/100, % 100 -> 100.00, 33.3333333 -> 33.33
	[CurrentStart, CurrentEnd] = vc_interval_handler:current_interval(VcMeta), 
	CurrentIntervalKey = {DcId,MetaId, CurrentStart},
	[NextStart, _NextEnd] = vc_interval_handler:next_interval(MetaInterval,CurrentStart,CurrentEnd),
 	NextIntervalKey = {DcId,MetaId, NextStart},
	IntervalUpdates = lists:foldl( fun(IntervalKey, Result) ->
		case mnesia:dirty_read(vc_interval, IntervalKey) of
			[] ->
				Result;
			[ExistingInterval] ->
				#vc_interval{total=OldTotal,available=OldAvailable} = ExistingInterval,
				IncreasingAmount = NewTotal - OldTotal,  % total increasing in d.c. (may be minus-)
				NewAvailable = max(OldAvailable+IncreasingAmount, 0),
				UpdatedInterval = ExistingInterval#vc_interval{total=NewTotal, available=NewAvailable},
				Result ++ [UpdatedInterval]
		end
	end,[],[CurrentIntervalKey,NextIntervalKey]),
	IntervalUpdates.
	
updating_initial_interval(VcMeta) ->  % VcMeta has initial total with it
	#vc_meta{id=MetaId,total=MetaTotal} = VcMeta,
	{ok, DcId} = application:get_env(vc_master, dc_id), 
	{ok, DcIds} = application:get_env(vc_master, dc_ids), 
	NewTotal = round( MetaTotal/length(DcIds)*100)/100, % 100 -> 100.00, 33.3333333 -> 33.33
	[CurrentStart, _CurrentEnd] = vc_interval_handler:current_interval(VcMeta), 
	CurrentIntervalKey = {DcId,MetaId, CurrentStart},
	[ExistingInterval] = mnesia:dirty_read(vc_interval, CurrentIntervalKey),
	#vc_interval{total=OldTotal,available=OldAvailable} = ExistingInterval,
	NewAvailable = max(OldAvailable+(NewTotal - OldTotal), 0),
	UpdatingCurrentInterval = ExistingInterval#vc_interval{total=NewTotal, available=NewAvailable},
	UpdatingCurrentInterval.
	
% this should not happen, but when happens, need to do self-recovery.
create_missing_intervals() ->
	{ok, DcId} = application:get_env(vc_master, dc_id), 
	CurrentTime = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
	MatchSpec = [
		_MatchSpec = {#vc_meta{start='$1', 'end'='$2', _='_'},
		_Guard =[{'=<','$1',CurrentTime},{'>', '$2', CurrentTime}],
		_Result = ['$_']}],
	mnesia:transaction(fun() ->
		lists:foreach( fun(#vc_meta{id=MetaId}=VcMeta) ->
			[CurrentStart, _] = vc_interval_handler:current_interval(VcMeta),
			case mnesia:read(vc_interval, {DcId,MetaId, CurrentStart}) of
				[] ->
					error_logger:warning_msg("vc_interval_handler: creating missing intervals for meta ~p",[VcMeta]),
					gen_server:cast(self(),{'create_new_intervals',VcMeta});
				_Any -> 
					ok
			end
		end, mnesia:select(vc_meta, MatchSpec, read))
	end),
	ok.

add_stats(Key,State=#state{stats=Stats}) ->
	NewStats = lists:map( fun({K,V}) ->
		if 
			K == Key -> {K,V+1};
			true     -> {K,V}
		end
	end, Stats),
	%error_logger:info_msg("vc_interval_handler: Stats ~500p",[NewStats]),
	State#state{stats=NewStats}.


