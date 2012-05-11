%................................................
%. Remote Handler RabbitMQ message handling test
%................................................
-module(remote_handler_rabbitmq_test).
-include_lib("../include/records.hrl").
-include_lib("../amqp_client-2.6.1/include/amqp_client.hrl").
-compile(export_all).

% Eunit has too much timeout limit, therefore we made our own test.
test() ->
	volumecap_test:setup(),
	available_rabbitmq_connect_test(),
	interval_update_out_to_22_test(),
	interval_update_in_from_22_test(),
	toup_request_out_to_22_test(),
	toup_response_in_from_22_test(),
	toup_request_in_from_22_test(),
	ok.

available_rabbitmq_connect_test() ->
	?DEBUG("... available rabbitmq connect test"),
	{MasterNode, _VolumecapNode} = volumecap_test:reset(),
	rpc:call(MasterNode, application, set_env, [vc_master, remote_queues, [ 
		[{host,"unavailable.host"}, {vhost,<<"/foo">>}, {queue,<<"bar">>}, {exchange,<<"foobar">>}],
		[{host,"localhost"}, {vhost,<<"/prod_12">>}, {queue,<<"vc_remote_12">>}, {exchange,<<"vc_remote">>}]
	] ]),
	rpc:call(MasterNode, vc_master, restart, [vc_remote_handler]), 
	timer:sleep(500),
	vc_master_test:wait_until(fun() ->
		{_,_Dc,MQConn,MQChannel,MQExchange,_Stats} = rpc:call(MasterNode,gen_server, call, [{global,vc_remote_handler_12},'get_state']),
		true=is_pid(MQConn),
		true=is_pid(MQChannel),
		<<"vc_remote">>=MQExchange
	end, 5),
	?DEBUG("... available rabbitmq connect test ok"),
	ok.

interval_update_out_to_22_test() ->
	?DEBUG("... interval_update_out_to_22_test"),
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	{MasterNode, _VolumecapNode} = volumecap_test:reset(),
	% purge the queue
	{_,_Dc,_Conn,MQChannel,_Ex,_Stats} = rpc:call(MasterNode,gen_server, call, [{global,vc_remote_handler_12},'get_state']),
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_22">>}]),
	% create a new vc_interval
	VcInterval = #vc_interval{'id'={12,1234567890,Today},'dc_id'=12,'start'=Today,'end'=Today+86400*10,'total'=500.00,'available'=450.00,'consumed'=50.00},
	ok = rpc:call(MasterNode, mnesia, dirty_write, [VcInterval]),    
	timer:sleep(100),
	% read vc_remote_22, we need to have two interval updates for new(500/0)/ two interval updates for topup(450/50).
	{#'basic.get_ok'{}, #amqp_msg{payload=Payload}}= rpc:call(MasterNode, amqp_channel,call, [MQChannel, #'basic.get'{queue= <<"vc_remote_22">>, no_ack=true}]),
	{write, VcInterval, {dirty,_}} = binary_to_term(Payload),
	?DEBUG("... interval_update_out_to_22_test ... ok"),
	ok.

interval_update_in_from_22_test() ->
	?DEBUG("... interval_update_in_from_22_test"),
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	{MasterNode, _VolumecapNode} = volumecap_test:reset(),
	% purge the que
	{_,_Dc,_MQConnection,MQChannel,_Ex,_Stats} = rpc:call(MasterNode,gen_server, call, [{global,vc_remote_handler_12},'get_state']),
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_12">>}]),
	% publish a interval update message to vc_remote exchange
	RemoteVcInterval = #vc_interval{'id'={22,1111,Today},'dc_id'=22,'start'=Today,'end'=Today+86400*10,'total'=500.00,'available'=450.00,'consumed'=50.00},
	ok = rpc:call(MasterNode, amqp_channel, cast, [ MQChannel, #'basic.publish'{exchange= <<"vc_remote">>, routing_key= <<"12.vc_remote">>},
			#amqp_msg{props=#'P_basic'{}, payload=erlang:term_to_binary({write,RemoteVcInterval,{dirty,foo}})} ]),
	% vc_remote_handler will consume this message and write this interval
	timer:sleep(100),
	[RemoteVcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{22,1111,Today}]),    
	?DEBUG("... interval_update_in_from_22_test ... ok"),
	ok.

toup_request_out_to_22_test() ->
	?DEBUG("... toup_request_out_to_22_test"),
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	{MasterNode, _VolumecapNode} = volumecap_test:reset(),
	{_,_Dc,_Conn,MQChannel,_Ex,_Stats} = rpc:call(MasterNode,gen_server, call, [{global,vc_remote_handler_12},'get_state']),
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_22">>}]),
	% create a new cap
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	% received topup request to remote handler, and we found NO remote has available, then check if vc_remote_22 has NOT the request
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_22">>}]),
	ok = rpc:call(MasterNode,gen_server, cast, [{global, vc_remote_handler_12}, {'topup',{12,1111,Today},50}]),
	timer:sleep(100),
	{'basic.get_empty',<<>>} = rpc:call(MasterNode, amqp_channel,call, [MQChannel, #'basic.get'{queue= <<"vc_remote_22">>, no_ack=true}]),
	% received topup request from LH to remote handler, and we found remote has available, then check if vc_remote_22 has the request
	RemoteVcInterval = #vc_interval{'id'={22,1111,Today},'dc_id'=22,'start'=Today,'end'=Today+86400*10,'total'=500.00,'available'=450.00,'consumed'=50.00},
	ok = rpc:call(MasterNode, mnesia, dirty_write, [RemoteVcInterval]),    
	% now we have remote interval available
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_22">>}]),
	ok = rpc:call(MasterNode,gen_server, cast, [{global, vc_remote_handler_12}, {'topup',{12,1111,Today},50}]),
	[LocalVcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{12,1111,Today}]),
	{#'basic.get_ok'{}, #amqp_msg{payload=Payload}}= rpc:call(MasterNode, amqp_channel,call, [MQChannel, #'basic.get'{queue= <<"vc_remote_22">>, no_ack=true}]),
	{topup, LocalVcInterval, 50} = binary_to_term(Payload),
	?DEBUG("... topup_request_out_to_22_test ... ok"),
	ok.

toup_response_in_from_22_test() ->
	?DEBUG("... toup_response_in_from_22_test"),
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	{MasterNode, _VolumecapNode} = volumecap_test:reset(),
	{_,_Dc,_Conn,MQChannel,_Ex,_Stats} = rpc:call(MasterNode,gen_server, call, [{global,vc_remote_handler_12},'get_state']),
	% create a new cap
	LocalVcInterval  = #vc_interval{'id'={12,1111,Today},'dc_id'=12,'start'=Today,'end'=Today+86400*10,'total'=500.00,'available'=50.00,'consumed'=450.00},
	ok = rpc:call(MasterNode, mnesia, dirty_write, [LocalVcInterval]),    
	timer:sleep(100),
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_22">>}]),
	ok = rpc:call(MasterNode, amqp_channel, cast, [ MQChannel, #'basic.publish'{exchange= <<"vc_remote">>, routing_key= <<"12.vc_remote">>},
			#amqp_msg{props=#'P_basic'{}, payload=erlang:term_to_binary({'topup',LocalVcInterval,50.00})} ]),
	timer:sleep(100),
	% remote handler consumer reads top up message, and add it to vc_interval
	[NewLocalVcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{12,1111,Today}]),    
	550.00 = NewLocalVcInterval#vc_interval.total,
	100.00 = NewLocalVcInterval#vc_interval.available,
	450.00 = NewLocalVcInterval#vc_interval.consumed,
	?DEBUG("... toup_response_in_from_22_test ... ok"),
	ok.

toup_request_in_from_22_test() ->
	?DEBUG("... toup_request_in_from_22_test"),
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	{MasterNode, _VolumecapNode} = volumecap_test:reset(),
	{_,_Dc,_Conn,MQChannel,_Ex,_Stats} = rpc:call(MasterNode,gen_server, call, [{global,vc_remote_handler_12},'get_state']),
	% received topup request from RabbitMQ-vc_remote_12(remote handler), and we found interval available
	% check if vc_remote_22 has the response
	LocalVcInterval  = #vc_interval{'id'={12,1111,Today},'dc_id'=12,'meta_id'=1111,'start'=Today,'end'=Today+86400*10,'total'=500.00,'available'=450.00,'consumed'=50.00},
	RemoteVcInterval = #vc_interval{'id'={22,1111,Today},'dc_id'=22,'meta_id'=1111,'start'=Today,'end'=Today+86400*10,'total'=500.00,'available'=100.00,'consumed'=400.00},
	ok = rpc:call(MasterNode, mnesia, dirty_write, [LocalVcInterval]),    
	ok = rpc:call(MasterNode, mnesia, dirty_write, [RemoteVcInterval]),    
	timer:sleep(100),
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_22">>}]),
	ok = rpc:call(MasterNode, amqp_channel, cast, [ MQChannel, #'basic.publish'{exchange= <<"vc_remote">>, routing_key= <<"12.vc_remote">>},
			#amqp_msg{props=#'P_basic'{}, payload=erlang:term_to_binary({'topup',RemoteVcInterval,50.00})} ]),
	timer:sleep(100),
	{#'basic.get_ok'{}, #amqp_msg{payload=Payload}}= rpc:call(MasterNode, amqp_channel,call, [MQChannel, #'basic.get'{queue= <<"vc_remote_22">>, no_ack=true}]),
	{topup, RemoteVcInterval, 50.00} = binary_to_term(Payload),
	% received topup request from RabbitMQ-vc_remote_12(remote handler), and we found NO interval available
	% check if vc_remote_22 has NO response, since we don't have any. (50.00 is only enough for local)
	LocalVcIntervalNoAvailable = #vc_interval{'id'={12,1111,Today},'dc_id'=12,'meta_id'=1111,'start'=Today,'end'=Today+86400*10,'total'=500.00,'available'=50.00,'consumed'=450.00},
	ok = rpc:call(MasterNode, mnesia, dirty_write, [LocalVcIntervalNoAvailable]),
	timer:sleep(100),
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_22">>}]),
	ok = rpc:call(MasterNode, amqp_channel, cast, [ MQChannel, #'basic.publish'{exchange= <<"vc_remote">>, routing_key= <<"12.vc_remote">>},
			#amqp_msg{props=#'P_basic'{}, payload=erlang:term_to_binary({'topup',RemoteVcInterval,50.00})} ]),
	timer:sleep(100),
	{'basic.get_empty',<<>>} = rpc:call(MasterNode, amqp_channel,call, [MQChannel, #'basic.get'{queue= <<"vc_remote_22">>, no_ack=true}]),
	?DEBUG("... toup_request_in_from_22_test ... ok"),
	ok.

