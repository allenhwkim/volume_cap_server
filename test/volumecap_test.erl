%.............................................................
%. volumecap test ... common functions for volumecap testing
%.............................................................
-module(volumecap_test).
-include_lib("../include/records.hrl").
-include_lib("../amqp_client-2.6.1/include/amqp_client.hrl").
-compile(export_all).

% Eunit has too much timeout limit, therefore we made our own test.
test() ->
	setup(),
	ok.

setup() ->
	setup(_Forcefully=false).
setup(Forcefully) ->
	try
		false=Forcefully,
		MasterStatus = rpc:call('vctest1@127.0.0.1', vc_master, status,[]),
		[_,_] = proplists:get_value(master_nodes, MasterStatus),
		{_,_,_,_,_,_,A,B,C,D,_,_,_} = proplists:get_value(primary_state, MasterStatus), 
		{true,true,true,true}= {erlang:is_pid(A), erlang:is_pid(B), erlang:is_pid(C), erlang:is_pid(D)},
		{state, _, _} = proplists:get_value(secondary_state, MasterStatus),              
		{state,_,_}   = gen_server:call({volumecap_server, 'vctest3@127.0.0.1'}, get_state)
	catch Class:Reason ->
		?DEBUG("Class ~p Reason ~p",[Class,Reason]),
		?DEBUG("... setup"),
		vc_master_test:stop_master_node('vctest1@127.0.0.1'),
		vc_master_test:stop_master_node('vctest2@127.0.0.1'),
		vc_master_test:stop_master_node('vctest3@127.0.0.1'),
		volumecap_test:stop_volumecap_node('vctest3@127.0.0.1'),
		vc_master_test:start_master_node('vctest1@127.0.0.1'),  % primary master
		vc_master_test:start_master_node('vctest2@127.0.0.1'),  % secondary master
		volumecap_test:start_volumecap_node('vctest3@127.0.0.1'),  % volumecap server
		rpc:call('vctest1@127.0.0.1',application,load,[vc_master]),
		rpc:call('vctest1@127.0.0.1',application,set_env,[vc_master,master_nodes,['vctest1@127.0.0.1','vctest2@127.0.0.1']]),
		rpc:call('vctest2@127.0.0.1',application,load,[vc_master]),
		rpc:call('vctest2@127.0.0.1',application,set_env,[vc_master,master_nodes,['vctest1@127.0.0.1','vctest2@127.0.0.1']]),
		ok = rpc:call('vctest1@127.0.0.1', vc_master, start, []),
		PrimarySecondaryReady = fun() ->
			Status = rpc:call('vctest1@127.0.0.1', vc_master, status,[]),
			[_,_] = proplists:get_value(master_nodes, Status),
			{_,_Dc,_SM,_SMN,_SMS,_SS,MH,IH,LH,RH,_PM,_DLII,_DLSI} = proplists:get_value(primary_state, Status), 
			{true,true,true,true}= {erlang:is_pid(MH), erlang:is_pid(IH), erlang:is_pid(LH), erlang:is_pid(RH)},
			{state, _, 0} = proplists:get_value(secondary_state, Status),                 % secondary is active
			ok
		end,
		vc_master_test:wait_until(PrimarySecondaryReady, 10),
		{ok, _} = rpc:call('vctest3@127.0.0.1', volumecap_server, start, []),
		%
		ok = rpc:call('vctest1@127.0.0.1', vc_master_test, set_rabbitmq, []),
		?DEBUG("... setup ... ok")
	end.

reset() ->
	MasterNode = 'vctest1@127.0.0.1',
	VolumecapNode = 'vctest3@127.0.0.1',
	{atomic,ok} = rpc:call(MasterNode,mnesia,clear_table, [vc_meta]),
	{atomic,ok} = rpc:call(MasterNode,mnesia,clear_table, [vc_interval]),
	{atomic,ok} = rpc:call(VolumecapNode,mnesia,clear_table, [volumecap]),
	rpc:call(MasterNode, gen_server,call,[{global,vc_interval_handler_12}, 'reset_stats']),
	rpc:call(MasterNode, gen_server,call,[{global,vc_local_handler_12}, 'reset_stats']),
	gen_server:call({volumecap_server, VolumecapNode}, 'reset_stats'),
	{MasterNode, VolumecapNode}.

teardown() ->
	?DEBUG("... teardown"),
	vc_master_test:stop_master_node('vctest1@127.0.0.1'),
	vc_master_test:stop_master_node('vctest2@127.0.0.1'),
	vc_master_test:stop_master_node('vctest3@127.0.0.1'),
	volumecap_test:stop_volumecap_node('vctest3@127.0.0.1'),
	?DEBUG("... teardown ... ok").

start_volumecap_node(Node) ->
	CompileOptions = proplists:get_value(options, volumecap_server:module_info(compile)),
	EbinDir = proplists:get_value(outdir, CompileOptions),
	Command = "erl -detached -name "++atom_to_list(Node)
		++" -setcookie vctest  "
		++" -mnesia dir '\"/tmp/"++atom_to_list(Node)++"\"' "
		++" -pa "++EbinDir
		++" -boot start_sasl"
		++" -sasl sasl_error_logger '{file,\"/tmp/"++atom_to_list(Node)++".sasl.log\"}'"
		++" -kernel error_logger '{file,\"/tmp/"++atom_to_list(Node)++".kernel.log\"}'"
		++" -run mnesia",
	?DEBUG(Command),
	os:cmd("rm -rf /tmp/"++atom_to_list(Node)),
	[] = os:cmd(Command),
	timer:sleep(500),
	ConnectToMasterNodes = fun() ->
		{ok, MasterNodes} = application:get_env(vc_master, master_nodes),
		lists:foreach(fun(MasterNode) ->
			pong = rpc:call(Node, net_adm, ping, [MasterNode])
		end, MasterNodes)
	end,
	vc_master_test:wait_until(ConnectToMasterNodes, 5),
	?DEBUG("Now volumecap node(~p) is started",[Node]),
	Node.

stop_volumecap_node(Node) ->
	rpc:call(Node, init, stop, []),
	vc_master_test:wait_until( fun() -> pang = net_adm:ping(Node) end, 5),
	ok.
	
