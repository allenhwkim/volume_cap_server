-module(vc_remote_handler).
-include("include/records.hrl").
-include_lib("amqp_client-2.6.1/include/amqp_client.hrl").
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-define(STATS, [{topup_req,0},{topup_res,0},{dupe_in,0},{dupe_out,0},{mq_reconnect,0}]).
-record(state, {dc_id, mq_connection, mq_channel, mq_exchange, stats=?STATS}).

start_link() ->
	{ok,_Pid} = gen_server:start_link({global, vc_master:handler_name(remote)}, ?MODULE, [], []).

init([]) ->
	process_flag(trap_exit, true),
	ok = vc_master:wait_for_primary_vc_master_ready(0),
	error_logger:info_msg("vc_remote_handler: connecting to RabbitMQ"),
	{Connection, Channel, Exchange} = connect_and_consume(),
	{ok, DcId} = application:get_env(vc_master, dc_id),
	error_logger:info_msg("vc_remote_handler: Subscribing to mnesia table,vc_interval"),
	mnesia:subscribe({table, vc_interval, simple}),
	{ok, #state{
			dc_id=DcId,  
			mq_connection=Connection, 
			mq_channel=Channel, 
			mq_exchange=Exchange}}.

handle_call('get_state', _From, State)  ->
	{reply, State, State};

handle_call('reset_stats', _From, State)  ->
	NewState = State#state{stats=?STATS},
	{reply, NewState, NewState};

handle_call(Unexpected, From, State) ->
	error_logger:info_msg("vc_remote_handler: Unexpected handle_call ~p from ~p",[Unexpected,From]),
	{reply, ignored, State}.

% get the richest d.c. id within the interval
% send 'topup' quest to RabbitMQ
handle_cast({'topup',{_,_,_}=VcIntervalId,Amount}, State) -> 
	[LocalVcInterval] = mnesia:dirty_read(vc_interval,VcIntervalId),
	handle_cast({'topup',LocalVcInterval,Amount}, State);
handle_cast({'topup',LocalVcInterval,Amount}, State) -> % from LH
	error_logger:info_msg("vc_remote_handler: <-LH, received topup request ~500p, amount ~p",[LocalVcInterval,Amount]),
	#vc_interval{meta_id=MetaId,start=Start} = LocalVcInterval,
	{ok, LocalDcId} = application:get_env(vc_master,dc_id),
	{ok, DcIds} = application:get_env(vc_master, dc_ids),
	RemoteAvailables = lists:foldl(fun(RemoteDcId, Result) ->
		RemoteVcIntervalKey = {RemoteDcId,MetaId,Start},
		case mnesia:dirty_read(vc_interval,RemoteVcIntervalKey) of
			[RemoteVcInterval] when RemoteVcInterval#vc_interval.available >= (Amount*2) ->
				Result ++ [{RemoteDcId,RemoteVcInterval#vc_interval.available}];
			_Any -> 
				Result
		end
	end, [], (DcIds -- [LocalDcId])),
	case RemoteAvailables of
		[] -> ok;
		_Any ->
			{MaxDcId, _Available} = lists:last(lists:keysort(2,RemoteAvailables)),
			TopupRequest = {'topup',LocalVcInterval,Amount},
			send_remote_topup_request(MaxDcId, TopupRequest, State),
			error_logger:info_msg("vc_remote_handler: ->RH(~p), sending remote topup request, ~500p",[MaxDcId,TopupRequest])
	end,
	NewState = add_stats('topup_req',State),
	{noreply, NewState};

handle_cast(Unexpected, State) -> 
	error_logger:info_msg("vc_remote_handler: Unexpected handle_cast message ~p",[Unexpected]),
	{noreply, State}.

% {write,  NewRecord, ActivityId},  {delete, {Tab, key}, ActivityId}, {delete_object, OldRecord, ActivityId}
handle_info({mnesia_table_event, Event}, State=#state{dc_id=DcId}) ->
	% send this d.c. vc_interval updates to other d.c.
	EventType = case Event of
		{'write', {schema, _, _}, _ActivityId} -> 'ignored';
		{'delete', {schema, _}, _ActivityId}   -> 'ignored';
		{'write', Record,_} when (Record#vc_interval.dc_id == DcId)            -> 'local_interval_update';
		{'delete', {vc_interval, {DcId,_,_},_}} when DcId == State#state.dc_id -> 'local_interval_update';
		{'delete_object',Record,_} when  (Record#vc_interval.dc_id == DcId)    -> 'local_interval_update';
		{'write', Record,_} when (Record#vc_interval.dc_id /= DcId)            -> 'remote_interval_update';
		{'delete', {vc_interval, {DcId,_,_},_}} when DcId == State#state.dc_id -> 'remote_interval_update';
		{'delete_object',Record,_} when  (Record#vc_interval.dc_id == DcId)    -> 'remote_interval_update';
		_Other ->
			error_logger:warning_msg("vc_remote_handler: Unexpected mnesia table event caught by a subscriber ~p",[Event]),
			'unknown'
	end,
	NewState = case EventType of
		'local_interval_update' ->
			{ok, DcIds} = application:get_env(vc_master, dc_ids),
			RemoteDcIds = lists:usort(DcIds -- [State#state.dc_id]),
			lists:foreach( fun(RemoteDcId) ->
				RoutingKey = list_to_binary(lists:concat([RemoteDcId,".vc_remote"])),
				ok = publish_rabbitmq_message(RoutingKey, term_to_binary(Event), State)
			end, RemoteDcIds),
			add_stats('dupe_out',State);
		_Any ->
			State
	end,
	{noreply, NewState};

% RabbitMQ message received.
handle_info({Frame, Content}, State=#state{dc_id=DcId}) when is_record(Frame, 'basic.deliver') and is_record(Content, amqp_msg) ->
	{'basic.deliver', _ConsumerTag, DeliveryTag, _ReDelivered, _Exchange, _RoutingKey} = Frame,
	PayloadMessage = binary_to_term(Content#amqp_msg.payload),
	error_logger:info_msg("vc_remote_handler: <-MQ, received request ~500p",[PayloadMessage]),
	NewState = try
		NewStateTmp = case PayloadMessage of
			{'topup',RemoteVcInterval,Amount} when RemoteVcInterval#vc_interval.dc_id /= DcId ->  % topup request from remote d.c.
				error_logger:info_msg("vc_remote_handler: <-MQ, received topup request from remote d.c. ~500p",[PayloadMessage]),
				case get_interval_for_remote_topup({'topup',RemoteVcInterval,Amount}) of          % we need to respond back if available
					error -> ok;
					NewLocalVcInterval -> 
						mnesia:dirty_write(NewLocalVcInterval),
						RemoteDcId = RemoteVcInterval#vc_interval.dc_id ,
						ok = send_remote_topup_response(RemoteDcId, {'topup',RemoteVcInterval,Amount}, State)
				end,
				add_stats('topup_req',State);
			{'topup',LocalVcInterval,Amount} when LocalVcInterval#vc_interval.dc_id == DcId ->  % topup response from remote d.c.
				error_logger:info_msg("vc_remote_handler: <-MQ, received topup response from remote d.c. ~500p",[PayloadMessage]),
				case mnesia:dirty_read(vc_interval, LocalVcInterval#vc_interval.id) of     % we need to add to available amount
					[] -> ok; % this must not happen
					[VcInterval] ->
						#vc_interval{total=Total,available=Available} = VcInterval,
						mnesia:dirty_write(VcInterval#vc_interval{total=Total+Amount, available=Available+Amount})
				end,
				add_stats('topup_res',State);
			MnesiaEvent ->                               % mnesia dupl event requet from remote d.c.
				error_logger:info_msg("vc_remote_handler: <-MQ, received interval dupl. mnesia table from remote d.c. ~500p",[PayloadMessage]),
				ok = duplicate_mnesia_event(MnesiaEvent),
				add_stats('dupe_in',State)
		end,
		BasicAck = #'basic.ack'{delivery_tag = DeliveryTag, multiple = false},
		ok = amqp_channel:cast(State#state.mq_channel, BasicAck),
		NewStateTmp
	catch
		Class:Error ->
			error_logger:error_report(["vc_remote_handler: Error processing amqp message", 
				{class, Class}, {error, Error}, {message,PayloadMessage}]),
			error_logger:error_report(["vc_remote_handler: Backtrace", {backtrace, erlang:get_stacktrace()}]),
			State
	end,
	{noreply, NewState};

% reconnect by timer
handle_info('reconnect', State) ->
%error_logger:info_msg("vc_remote_handler: reconnectiong with State ~500p",[State]),
	{Connection, Channel, Exchange} = connect_and_consume(),
	{noreply, State#state{mq_connection=Connection, mq_channel=Channel, mq_exchange=Exchange}};
	
% linked channel or connection dies, need to reconnect
handle_info({'EXIT', Pid, ExitMessage}, State) ->
	error_logger:info_msg("vc_remote_handler: DETECTED RabbitMQ CONNECTION(~p) DIED WITH ~p. RECONNECTING", [Pid,ExitMessage]),
	{Connection, Channel, Exchange} = connect_and_consume(),
	{noreply, State#state{mq_connection=Connection, mq_channel=Channel, mq_exchange=Exchange}};

handle_info(_Any, State) -> {noreply, State}.

terminate(_OtherReason, State) ->
	amqp_channel:close(State#state.mq_channel),
	amqp_connection:close(State#state.mq_connection),
	ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

% connection and channel are linked to this process, they will send 'EXIT' when disconnected
connect_and_consume() ->
    {ok, RemoteQConfigs} = application:get_env(vc_master, remote_queues),
	{Connection, Channel, Exchange} = lists:foldl( fun(RemoteQConfig, Result) ->
		case Result of
			{'undefined','undefined','undefined'} ->
				try 
					Host     = proplists:get_value('host', RemoteQConfig, "localhost"),
					Port     = proplists:get_value('port', RemoteQConfig, 5672),
					Vhost    = proplists:get_value('vhost', RemoteQConfig, <<"/prod_12">>),
					Queue12  = proplists:get_value('queue_12', RemoteQConfig, <<"vc_remote_12">>),
					Queue22  = proplists:get_value('queue_22', RemoteQConfig, <<"vc_remote_22">>),
					MQExchange = proplists:get_value('exchange', RemoteQConfig, <<"vc_remote">>),
					{ok, MQConnection} = amqp_connection:start(#amqp_params_network{
						host=Host, port=Port, virtual_host=Vhost, heartbeat = 60}),
					erlang:link(MQConnection),
					{ok, MQChannel} = amqp_connection:open_channel(MQConnection),
					erlang:link(MQChannel),
					%
					ExchangeDeclare = #'exchange.declare'{exchange=MQExchange, type= <<"topic">>, durable=true},
					QueueDeclare12 = #'queue.declare'{queue=Queue12,durable=true},
					QueueBind12 = #'queue.bind'{queue=Queue12, exchange=MQExchange, routing_key= <<"12.vc_remote.#">>},
					QueueDeclare22 = #'queue.declare'{queue=Queue22,durable=true},
					QueueBind22 = #'queue.bind'{queue=Queue22, exchange=MQExchange, routing_key= <<"22.vc_remote.#">>},
					%
					% declare queue/exchange/bindings
					#'exchange.declare_ok'{} = amqp_channel:call(MQChannel, ExchangeDeclare),
					#'queue.declare_ok'{} = amqp_channel:call(MQChannel, QueueDeclare12),
					#'queue.declare_ok'{} = amqp_channel:call(MQChannel, QueueDeclare22),
					#'queue.bind_ok'{} = amqp_channel:call(MQChannel, QueueBind12),
					#'queue.bind_ok'{} = amqp_channel:call(MQChannel, QueueBind22),
					%
					BasicConsume = #'basic.consume'{queue = Queue12},
					#'basic.consume_ok'{consumer_tag = _Tag} = amqp_channel:subscribe(MQChannel, BasicConsume, self()),
					%
					{MQConnection, MQChannel, MQExchange}
				catch _Class:Error ->
					error_logger:error_report(["vc_remote_handler: RabbitMQ Connection Error",{remote_queue, RemoteQConfig},{error,Error}]),
					{'undefined','undefined','undefined'}
				end;
			{MQConnection, MQChannel, MQExchange} -> 
				{MQConnection, MQChannel, MQExchange}
		end
	end, {'undefined','undefined','undefined'}, RemoteQConfigs),
	if 
		Connection == undefined; Channel == undefined -> 
			timer:send_after(5000, self(), 'reconnect');
		true -> 
			ok 
	end,
	{Connection, Channel, Exchange}.

duplicate_mnesia_event(Event) -> 
	Result = mnesia:transaction( fun() ->
		case Event of
			{'write', {schema, _, _}, _ActId}    -> ignored;
			{'delete', {schema, _}, _ActId}      -> ignored;
			{'write', NewRecord, _ActId}         -> mnesia:write(NewRecord);
			{'delete', {Tab, Key}, _ActId}       -> mnesia:delete({Tab, Key});
			{'delete_object', OldRecord, _ActId} -> mnesia:delete_object(OldRecord);
			Error ->
				error_logger:error_msg("vc_remote_handler: <-MQ, Unexpected RabbitMQ Message ~p",[Event]),
				Error
		end
	end),
	if 
		Result == {atomic,ok}; Result == 'ignored' -> 
			ok;
		true ->
			error_logger:error_msg("vc_remote_handler: Mnesia update failed with reason ~p",[Result]),
			{error, mnesia_update_failed}
	end.

get_interval_for_remote_topup({'topup',RemoteVcInterval,Amount}) ->
	{ok, DcId} = application:get_env(vc_master, dc_id),
	#vc_interval{meta_id=MetaId, start=Start} = RemoteVcInterval,
	NewLocalVcInterval = case mnesia:dirty_read(vc_interval, {DcId,MetaId,Start}) of
		[LocalVcInt] when LocalVcInt#vc_interval.available >= (Amount*2) ->
			#vc_interval{total=Total,available=Available} = LocalVcInt,
			LocalVcInt#vc_interval{'total'=(Total-Amount), 'available'=(Available-Amount)};
		_Any -> error 
	end,
	NewLocalVcInterval.

send_remote_topup_request(DcId, {'topup',_LocalVcInterval,_Amount}=TopupRequest, State) ->
	RoutingKey = list_to_binary(lists:concat([integer_to_list(DcId),".","vc_remote"])),
	ok = publish_rabbitmq_message(RoutingKey, TopupRequest, State).

send_remote_topup_response(DcId, {'topup',_RemoteVcInterval,_Amount}=TopupResponse, State) ->
	RoutingKey = list_to_binary(lists:concat([integer_to_list(DcId),".","vc_remote"])),
	ok = publish_rabbitmq_message(RoutingKey, TopupResponse, State).

publish_rabbitmq_message(RoutingKey, Message, State) when not is_binary(Message) ->
	publish_rabbitmq_message(RoutingKey, term_to_binary(Message), State);
publish_rabbitmq_message(RoutingKey, Message, State) ->
	Publish = #'basic.publish'{exchange=State#state.mq_exchange, routing_key = RoutingKey},
	Props = #'P_basic'{},
	AmqpMessage = #amqp_msg{props = Props, payload=Message},
	ok = amqp_channel:cast(State#state.mq_channel, Publish, AmqpMessage).

add_stats(Key,State=#state{stats=Stats}) ->
	NewStats = lists:map( fun({K,V}) ->
		if 
			K == Key -> {K,V+1};
			true     -> {K,V}
		end
	end, Stats),
	%error_logger:info_msg("vc_remote_handler: Stats ~500p",[NewStats]),
	State#state{stats=NewStats}.

