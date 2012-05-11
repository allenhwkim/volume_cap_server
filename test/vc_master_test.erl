-module(vc_master_test).
-include_lib("../include/records.hrl").
-include_lib("../amqp_client-2.6.1/include/amqp_client.hrl").
-compile(export_all).

% Eunit has too much timeout limit, therefore we made our own test.
test() ->
	setup_test(),
	start_primary_test(),
	start_secondary_test(),      % start secondary manually
	sync_primary_to_secondary_test(),
	stop_primary_test(),         % secondary take over primary
	auto_start_secondary_test(), % automatic start of secondary by primary
	cleanup_test().

setup_test() ->
	?DEBUG("... setup_test"),
	?DEBUG("... setup_test ... ok").
 
start_primary_test() ->
	?DEBUG("... start_primary_test"),
	PrimaryMasterNode  = 'vctest1@127.0.0.1',
	stop_master_node(PrimaryMasterNode),
	start_master_node(PrimaryMasterNode),
	rpc:call('vctest1@127.0.0.1',application,load,[vc_master]),
	rpc:call('vctest1@127.0.0.1',application,set_env,[vc_master,master_nodes,['vctest1@127.0.0.1']]),
	pong=net_adm:ping(PrimaryMasterNode),
	PrimaryMasterName = vc_master:master_name(primary),
	timer:sleep(1000), % wait for mnesia to start
	ok = rpc:call(PrimaryMasterNode, vc_master, start, []),
	% supervisor started all children
	WaitFun = fun() ->
		Children = rpc:call(PrimaryMasterNode, supervisor, which_children, [vc_master_sup] ),
		5 =  erlang:length(Children) % master + 4 handlers
	end,
	wait_until(WaitFun,20),
	% mnesia tables are created properly
	set=rpc:call(PrimaryMasterNode, mnesia, table_info, [vc_meta, type]),
	set=rpc:call(PrimaryMasterNode, mnesia, table_info, [vc_interval, type]),
	% four handlers are started
	WaitFun3 = fun() ->
		State = rpc:call(PrimaryMasterNode, gen_server, call, [{global,PrimaryMasterName}, get_state]),
		{_,_Dc,_SM,_SMN,_SMS,_SS,MH,IH,LH,RH,PM,_DLII,_DLSI} = State,
		true= erlang:is_pid(MH), %Meta Handler
		true= erlang:is_pid(IH), %Interval Handler
		true= erlang:is_pid(LH), %Local Handler
		true= erlang:is_pid(RH), %Remote Handler
		% process monitor is started
		{interval,_} = PM
	end,
	wait_until(WaitFun3,5),
	ok = rpc:call(PrimaryMasterNode, vc_master_test, set_rabbitmq, []),
	?DEBUG("... start_primary_test ... ok").

start_secondary_test() ->
	?DEBUG("... start_secondary_test"),
	PrimaryMasterNode = 'vctest1@127.0.0.1',
	SecondaryMasterNode  = 'vctest2@127.0.0.1',
	PrimaryMasterName = vc_master:master_name(primary),
	stop_master_node(SecondaryMasterNode),
	start_master_node(SecondaryMasterNode),
	rpc:call('vctest1@127.0.0.1',application,load,[vc_master]),
	rpc:call('vctest1@127.0.0.1',application,set_env,[vc_master,master_nodes,['vctest1@127.0.0.1','vctest2@127.0.0.1']]),
	rpc:call('vctest2@127.0.0.1',application,load,[vc_master]),
	rpc:call('vctest2@127.0.0.1',application,set_env,[vc_master,master_nodes,['vctest1@127.0.0.1','vctest2@127.0.0.1']]),
	timer:sleep(1000), % give some time for mnesia to start
	pong=net_adm:ping(SecondaryMasterNode),
	ok = rpc:call(SecondaryMasterNode, vc_master, start, []),
	State = rpc:call(PrimaryMasterNode, gen_server, call, [{global,PrimaryMasterName}, get_state]),
	% the secondary related state are all defined
	{_,_Dc,SM,SMN,SMS,SS,_MH,_IH,_LH,_RH,_PM,DLII,DLSI} = State,
	true= erlang:is_pid(SM),   % Secondary Master
	SecondaryMasterNode = SMN , % Secondary Master Node
	ready = SMS,               % Secondary Master Status
	{interval,_} = SS ,        % Secondary Synchronizer
	0 = DLII,                  % Volumecap DETS Last Insert Id
	0 = DLSI,                  % Volumecap DETS Last Sync Id
	set=rpc:call(SecondaryMasterNode, mnesia, table_info, [vc_meta, type]),
	set=rpc:call(SecondaryMasterNode, mnesia, table_info, [vc_interval, type]),
	?DEBUG("... start_secondary_test ... ok").  

sync_primary_to_secondary_test() ->
	?DEBUG("... sync_primary_to_secondary_test"),
	PrimaryMasterNode = 'vctest1@127.0.0.1',
	SecondaryMasterNode = 'vctest2@127.0.0.1',
	% mnesia has a subscriber and it is the primary master
	[Subscriber|_Rest] = rpc:call(PrimaryMasterNode, mnesia, table_info, [vc_interval, subscribers]),
	PrimaryMasterNode= erlang:node(Subscriber),
	%
	MnesiaFun = fun() ->
		lists:foreach( fun(Num) ->
			mnesia:write(#vc_meta{id=Num,start=2,'end'=3,total=10,interval=day}),
			mnesia:write(#vc_interval{id=Num,start=2,'end'=3,dc_id=12,available=5})
		end, lists:seq(1,1000))
	end,
	{atomic,ok}= rpc:call(PrimaryMasterNode, mnesia, clear_table, [vc_meta]),
	{atomic,ok}= rpc:call(PrimaryMasterNode, mnesia, clear_table, [vc_interval]),
	{atomic,ok}= rpc:call(SecondaryMasterNode, mnesia, clear_table, [vc_meta]),
	{atomic,ok}= rpc:call(SecondaryMasterNode, mnesia, clear_table, [vc_interval]),
	{atomic,ok}= rpc:call(PrimaryMasterNode, mnesia, transaction, [MnesiaFun]),
	WaitFun = fun() ->
		1000= rpc:call(SecondaryMasterNode, mnesia, table_info, [vc_meta, size]),
		1000= rpc:call(SecondaryMasterNode, mnesia, table_info, [vc_interval, size])
	end,
	wait_until(WaitFun,15),
	?DEBUG("... sync_primary_to_secondary_test ... ok").

stop_primary_test() ->
	?DEBUG("... stop_primary_test"),
	PrimaryMasterNode = 'vctest1@127.0.0.1',
	SecondaryMasterNode = 'vctest2@127.0.0.1',
	stop_master_node(PrimaryMasterNode),
	%
	Fun = fun() ->
		VcMasterStatus = rpc:call(SecondaryMasterNode, vc_master, status,[]),
		%?DEBUG("State ~p",[VcMasterStatus]),
		['vctest2@127.0.0.1'] = proplists:get_value(master_nodes, VcMasterStatus),
		{state,_,_,_,_,_,_,_,_,_,_,_,_} = proplists:get_value(primary_state, VcMasterStatus),
		'undefined' = proplists:get_value(secondary_state, VcMasterStatus),
		ok
	end,
	wait_until(Fun, 10),
	?DEBUG("... stop_primary_test ... ok").

auto_start_secondary_test() -> % start secondary by primary
	?DEBUG("... auto_start_secondary_test ..."),
	stop_master_node('vctest1@127.0.0.1'),
	stop_master_node('vctest2@127.0.0.1'),
	stop_master_node('vctest3@127.0.0.1'),
	start_master_node('vctest1@127.0.0.1'), 
	start_master_node('vctest2@127.0.0.1'), 
	start_master_node('vctest3@127.0.0.1'), 
	% start only primary, and see if there are primary and secondary
	MasterNodes = ['vctest1@127.0.0.1','vctest2@127.0.0.1','vctest3@127.0.0.1'],
	rpc:call('vctest1@127.0.0.1',application,load,[vc_master]),
	rpc:call('vctest2@127.0.0.1',application,load,[vc_master]),
	rpc:call('vctest3@127.0.0.1',application,load,[vc_master]),
	rpc:call('vctest1@127.0.0.1',application,set_env,[vc_master,master_nodes,MasterNodes]),
	rpc:call('vctest2@127.0.0.1',application,set_env,[vc_master,master_nodes,MasterNodes]),
	rpc:call('vctest3@127.0.0.1',application,set_env,[vc_master,master_nodes,MasterNodes]),
	ok = rpc:call('vctest1@127.0.0.1', vc_master, start, []),
	PrimarySecondarySet1 = fun() ->
		Status = rpc:call('vctest1@127.0.0.1', vc_master, status,[]),
		%?DEBUG("Status 1 ~p", [Status]),
		['vctest1@127.0.0.1','vctest2@127.0.0.1','vctest3@127.0.0.1'] = proplists:get_value(master_nodes, Status),
		{state,_,_,_,_,_,_,_,_,_,_,_,_} = proplists:get_value(primary_state, Status), % primary is active
		{state, _, 0} = proplists:get_value(secondary_state, Status),                 % secondary is active
		ok
	end,
	?DEBUG("... auto_start_secondary_test 1/2 ... ok"),
	wait_until(PrimarySecondarySet1, 10),
	% stop the primary node, and see if there are still primary and secondary
	stop_master_node('vctest1@127.0.0.1'),
	PrimarySecondarySet2 = fun() ->
		Status = rpc:call('vctest2@127.0.0.1', vc_master, status,[]),
		%?DEBUG("Status 2 ~p", [Status]),
		['vctest2@127.0.0.1','vctest3@127.0.0.1'] = proplists:get_value(master_nodes, Status),
		{state,_,_,_,_,_,_,_,_,_,_,_,_} = proplists:get_value(primary_state, Status), % primary is active
		{state, _, 0} = proplists:get_value(secondary_state, Status),                 % secondary is active
		ok
	end,
	wait_until(PrimarySecondarySet2, 10),
	?DEBUG("... auto_start_secondary_test 2/2... ok").

cleanup_test() ->
	?DEBUG("cleanup_test"),
	rpc:call('vctest1@127.0.0.1', init, stop, []),
	rpc:call('vctest2@127.0.0.1', init, stop, []),
	rpc:call('vctest3@127.0.0.1', init, stop, []),
	rpc:call('vctest4@127.0.0.1', init, stop, []).

stop_master_node(Node) ->
	rpc:call(Node, init, stop, []),
	wait_until( fun() -> pang = net_adm:ping(Node) end, 5),
	MasterNodes = case application:get_env(vc_master, master_nodes) of
		{ok,Nodes} -> Nodes; 
		undefined -> []
	end,
	NewMasterNodes = lists:usort(MasterNodes -- [Node]),
	application:set_env(vc_master, master_nodes, NewMasterNodes),
	lists:foreach( fun(MasterNode) ->
		rpc:call(MasterNode, application, set_env, [vc_master, master_nodes, NewMasterNodes])
	end, NewMasterNodes),
	ok.

start_master_node(Node) ->
	CompileOptions = proplists:get_value(options, vc_master:module_info(compile)),
	EbinDir = proplists:get_value(outdir, CompileOptions),
	Command = "erl -detached -name "++atom_to_list(Node)
		++" -setcookie vctest  "
		++" -mnesia dir '\"/tmp/"++atom_to_list(Node)++"\"' "
		++" -pa "++EbinDir
		++" -pa "++EbinDir++"/../*/ebin"
		++" -boot start_sasl"
		++" -sasl sasl_error_logger '{file,\"/tmp/"++atom_to_list(Node)++".sasl.log\"}'"
		++" -kernel error_logger '{file,\"/tmp/"++atom_to_list(Node)++".kernel.log\"}'"
		++" -run mnesia"
		++" -run vc_master_restarter ",
	?DEBUG(Command),
	os:cmd("rm -rf /tmp/"++atom_to_list(Node)),
	[] = os:cmd(Command),
	timer:sleep(500),
	wait_until(fun() -> pong=net_adm:ping(Node) end, 5),
	MasterNodes = case application:get_env(vc_master, master_nodes) of
		{ok,Nodes} -> Nodes; 
		undefined -> []
	end,
	application:set_env(vc_master, master_nodes, MasterNodes ++ [Node] ),
	rpc:call(Node, application, set_env, [vc_master, dc_id, 12]),
	rpc:call(Node, application, set_env, [vc_master, dc_ids, [12,22]]),
	rpc:call(Node, application, set_env, [vc_master, remote_queues, 
		[ [{host,"localhost"}, {vhost,<<"/prod_12">>}, {queue,<<"vc_remote_12">>}, {exchange,<<"vc_remote">>}] ] ]),
	ok.

wait_until(Fun, MaxSec) ->
	wait_until(Fun, MaxSec, 0).
wait_until(Fun, MaxSec, Sec) when MaxSec =< Sec ->
	io:format(user, "waiting failed for function ~p~n",[Fun]),
	error(timeout);
wait_until(Fun, MaxSec, Sec) when MaxSec > Sec ->
	try
		erlang:apply(Fun,[])
	catch _:Error ->
		timer:sleep(1000),
		io:format(user, "~p waiting ~p~n",[Error,Sec+1]),
		wait_until(Fun, MaxSec,Sec+1)
	end.

% this is for setting up test env quicky in console
set_env() ->
	application:set_env(vc_master, dc_id, 12),
	application:set_env(vc_master, dc_ids, [12,22]),
	application:set_env(vc_master, threshold_pct, 10),
	application:set_env(vc_master, remote_queues, 
		[ [{host,"localhost"}, {vhost,<<"/prod_12">>}, {queue,<<"vc_remote_12">>}, {exchange,<<"vc_remote">>}] ] ),
	application:set_env(vc_master, master_nodes, ['vcnode1@127.0.0.1','vcnode2@127.0.0.1']).

set_rabbitmq() ->
	{Host,VHost,Queue,Exchange} = {"localhost", <<"/">>, <<"vc_test">>,<<"vc_test">>},
	{ok, Connection}= amqp_connection:start(#amqp_params_network{host=Host, virtual_host=VHost}),
	{ok, Channel} = amqp_connection:open_channel(Connection),

	ExchangeDeclare = #'exchange.declare'{exchange=Exchange, type= <<"topic">>, durable=true},
	QueueDeclare = #'queue.declare'{queue=Queue,durable=true},
	QueueBind = #'queue.bind'{queue=Queue, exchange=Exchange, routing_key= <<"vc_test.#">>},

	%declare queue/exchange/bindings
	#'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),
	#'queue.declare_ok'{} = amqp_channel:call(Channel, QueueDeclare),
	#'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
	%#'queue.unbind_ok'{} = amqp_channel:call(Channel,#'queue.unbind'{queue=Queue, exchange=Exchanbe, routing_key= <<"12.vc_remote.#">>}),
	%#'exchange.delete_ok'{} = amqp_channel:call(Channel,  #'exchange.delete'{exchange=Exchange}),
	%#'queue.delete_ok'{} = amqp_channel:call(Channel, #'queue.delete'{queue=Queue}),

	%publish ok?
	Payload = erlang:term_to_binary({hello,world}),
	Publish = #'basic.publish'{exchange=Exchange, routing_key = <<"vc_test.hello_world">>},
	ok = amqp_channel:cast(Channel, Publish, #amqp_msg{props=#'P_basic'{delivery_mode=2}, payload=Payload} ),

	%get ok?
	{#'basic.get_ok'{}, Content} = amqp_channel:call(Channel, #'basic.get'{queue=Queue, no_ack = true}),
	#amqp_msg{payload=Payload} = Content,
	{hello,world} = binary_to_term(Payload),
	
	amqp_channel:close(Channel),
	amqp_connection:close(Connection),
	ok.
