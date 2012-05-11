%..............................
%. volumecap consumption test
%..............................
-module(volumecap_consumption_test).
-include_lib("../include/records.hrl").
-include_lib("../amqp_client-2.6.1/include/amqp_client.hrl").
-compile(export_all).

% Eunit has too much timeout limit, therefore we made our own test.
test() ->
	volumecap_test:setup(),
	volumecap_query_test(),
	volumecap_consumption_test(),
	volumecap_consume_all_one_by_one_test(),
	ok.

volumecap_query_test() ->
	?DEBUG("... volumecap query test"),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),
	% create a new cap
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	[IntervalBefore] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[VolumecapBefore] = rpc:call(VolumecapNode,mnesia,dirty_read,[volumecap,{12,1111,Today}]),     
	{error, not_found} = gen_server:call({volumecap_server, VolumecapNode}, {'query',2222}),
	{ok, Available} = gen_server:call({volumecap_server, VolumecapNode}, {'query',1111}),
	true  = (Available > 0),
	[IntervalAfter] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[VolumecapAfter] = rpc:call(VolumecapNode,mnesia,dirty_read,[volumecap,{12,1111,Today}]),     
	true = (IntervalBefore =:= IntervalAfter),
	true = (VolumecapBefore =:= VolumecapAfter),
	?DEBUG("... volumecap query test ok").

volumecap_consumption_test() ->
	?DEBUG("... volumecap consumption test"),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),
	% create a new cap
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	[IntervalBefore] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[_VolumecapBefore] = rpc:call(VolumecapNode,mnesia,dirty_read,[volumecap,{12,1111,Today}]),     
	{error, not_found} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',2222, 5.00}),
	{ok, Available} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',1111, 5.00}),
	true  = (Available > 0), % available after consumption is still high ehough
	[IntervalAfter] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[VolumecapAfter] = rpc:call(VolumecapNode,mnesia,dirty_read,[volumecap,{12,1111,Today}]),     
	true = (IntervalBefore =:= IntervalAfter),  % interval is not changed
	45.00 = VolumecapAfter#volumecap.available,
	5.00  = VolumecapAfter#volumecap.consumed,
	ok = rpc:call(VolumecapNode,mnesia,dirty_write,[VolumecapAfter#volumecap{available=0}]),  % forcing capped.    
	{error, capped} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',1111, 5.00}),
	?DEBUG("... volumecap consumption test ok").

% test to consume all interval availables from a single node one by one, by topping up from local handler
volumecap_consume_all_one_by_one_test() ->
	?DEBUG("... volumecap consume all one by one test"),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),
	{_,_Dc,_Conn,MQChannel,MQExchange,_Stats} = rpc:call(MasterNode,gen_server, call, [{global,vc_remote_handler_12},'get_state']),
	% create a new cap
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=100},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	% consume it 50 times by topping up every 5 
	lists:foreach( fun(X)->
		lists:foreach( fun(_Y) ->
			{ok, _} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',1111, 1.00}),
			?DEBUG("Volumecap ~500p",[rpc:call(VolumecapNode, mnesia,dirty_read,[volumecap,{12,1111,Today}])])
		end, lists:seq(1,5)),
		[Volumecap] = rpc:call(VolumecapNode, mnesia,dirty_read,[volumecap,{12,1111,Today}]),
		50.00 = Volumecap#volumecap.total,
		true = (X*5.00 == Volumecap#volumecap.consumed),
		timer:sleep(50) % give some time for topping up
	end, lists:seq(1,10)),
	{error,capped} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',1111, 1.00}),

	%?DEBUG("Volumecap Status ~p",[rpc:call(MasterNode, vc_master, status, [1111])]),
	[LocalVcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{12,1111,Today}]),
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_22">>}]),
	lists:foreach( fun(X)->
		AMQPMsg = #amqp_msg{props=#'P_basic'{}, payload=erlang:term_to_binary({'topup',LocalVcInterval,5.00})},
		rpc:call(MasterNode, amqp_channel, cast, [MQChannel, #'basic.publish'{exchange=MQExchange, routing_key= <<"12.vc_remote">>}, AMQPMsg]),
		vc_master_test:wait_until(fun()->  % remote topup will increase vc_interval.available
			[VcInt] = rpc:call(MasterNode, mnesia,dirty_read,[vc_interval,{12,1111,Today}]),
			5.00 = VcInt#vc_interval.available
		end, 5),
		rpc:call(MasterNode, gen_server, cast, [{global,vc_local_handler_12}, {'topup',{12,1111,Today},5.00,VolumecapNode}]),
		vc_master_test:wait_until(fun()-> % local handler will assign amount to volumecap server
			[Volumecap] = rpc:call(VolumecapNode, mnesia,dirty_read,[volumecap,{12,1111,Today}]),
			true = (Volumecap#volumecap.total == 50.00 + X*5),
			5.00  = Volumecap#volumecap.available
		end, 5),
		% let's consume all 5, that has been assigned
		lists:foreach( fun(_Y) ->
			{ok, _} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',1111, 1.00}),
			?DEBUG("Volumecap ~500p",[rpc:call(VolumecapNode, mnesia,dirty_read,[volumecap,{12,1111,Today}])])
		end, lists:seq(1,5))
	end, lists:seq(1,10)),
	{error,capped} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',1111, 1.00}),

	?DEBUG("... volumecap consume all one by one test ... ok"),
	ok.
