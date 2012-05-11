%..............................
%. update meta test
%..............................
-module(update_meta_test).
-include_lib("../include/records.hrl").
-compile(export_all).

% Eunit has too much timeout limit, therefore we made our own test.
test() ->
	volumecap_test:setup(),
	update_meta_with_same_data_test(),
	increase_meta_total_test(),
	decrease_meta_total_resulting_plus_available_test(),
	decrease_meta_total_resulting_no_available_test(),
	ok.

setup_test(false) ->
	ok;
setup_test(true) ->
	?DEBUG("... setup_test"),
	vc_master_test:stop_master_node('vctest1@127.0.0.1'),
	vc_master_test:stop_master_node('vctest2@127.0.0.1'),
	vc_master_test:stop_master_node('vctest3@127.0.0.1'),
	volumecap_test:stop_volumecap_node('vctest3@127.0.0.1'),
	vc_master_test:start_master_node('vctest1@127.0.0.1'),  % primary master
	vc_master_test:start_master_node('vctest2@127.0.0.1'),  % secondary master
	volumecap_test:start_volumecap_node('vctest3@127.0.0.1'),  % volumecap server
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
	?DEBUG("... setup_test ... ok").

update_meta_with_same_data_test() -> 
	?DEBUG("... update meta with same data test"),
	{MasterNode, _VolumecapNode} = volumecap_test:reset(),

	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	{state,IHStats1} = rpc:call(MasterNode, gen_server,call,[{global,vc_interval_handler_12},get_state]),
	1 = proplists:get_value(add_new, IHStats1),                                                 % IH stats.add_new stays the same
	0 = proplists:get_value(do_nothing, IHStats1),     

	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	{state,IHStats2} = rpc:call(MasterNode, gen_server,call,[{global,vc_interval_handler_12},get_state]),
	1 = proplists:get_value(add_new, IHStats2),                                                 % IH stats.add_new stays the same
	1 = proplists:get_value(do_nothing, IHStats2),                                              % IH stats.do_nothing increaes
	?DEBUG("... update meta with same data test ... ok"),
	ok.

increase_meta_total_test() ->
	?DEBUG("... increase meta total test"),
	{MasterNode, _VolumecapNode} = volumecap_test:reset(),

	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	{state,_,LHStats1} = rpc:call(MasterNode, gen_server,call,[{global,vc_local_handler_12},get_state]),
	[Int1] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[Int2] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today+86400}]),
	500.00 = Int1#vc_interval.total,
	500.00 = Int2#vc_interval.total,
	0 = proplists:get_value(b_update, LHStats1), 
	%
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta#vc_meta{total=2000}}]),
	timer:sleep(100),
	{state,_,LHStats2} = rpc:call(MasterNode, gen_server,call,[{global,vc_local_handler_12},get_state]),
	[Int3] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[Int4] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today+86400}]),
	1000.00 = Int3#vc_interval.total,
	1000.00 = Int4#vc_interval.total,
	2 = proplists:get_value(b_update, LHStats2), 
	?DEBUG("... increase meta total test ... ok"),
	ok.

decrease_meta_total_resulting_plus_available_test() ->
	?DEBUG("... decrease meta total resulting plus available test"),
	{MasterNode, _VolumecapNode} = volumecap_test:reset(),

	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta#vc_meta{total=500}}]),
	timer:sleep(100),
	{state,_,LHStats1} = rpc:call(MasterNode, gen_server,call,[{global,vc_local_handler_12},get_state]),
	[Int1] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[Int2] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today+86400}]),
	250.00 = Int1#vc_interval.total,             %        . active intervals are updated    
	250.00 = Int2#vc_interval.total,
	2 = proplists:get_value(b_update, LHStats1), %        . LH stats.b_update increases
	?DEBUG("... decrease meta total resulting plus available test ... ok"),
	ok.

%    . decrease meta total less than interval.available
%        . active intervals are updated with available from NN to 0 
%        . LH stats.b_update increases
%        . VS stats.b_update increases
%        . volumecap.available changes from NN to 0
decrease_meta_total_resulting_no_available_test() ->
	?DEBUG("... decrease meta total resulting no available test"),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),

	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	[IntA1] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[_IntA2] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today+86400}]),
    % change the current available from 450 to 250
	ok = rpc:call(MasterNode,mnesia,dirty_write,[IntA1#vc_interval{available=250,consumed=250}]), 
	% update meta total less than available
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta#vc_meta{total=400}}]),
	timer:sleep(100),
	{state,_,LHStats} = rpc:call(MasterNode, gen_server,call,[{global,vc_local_handler_12},get_state]),
	{state,_,VSStats}  = gen_server:call({volumecap_server, VolumecapNode}, get_state),
	[IntB1] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[IntB2] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today+86400}]),

	[Volumecap] = rpc:call(VolumecapNode, mnesia, dirty_read, [volumecap,{12,1111,Today}]),    
	[VolumecapTomorrow] = rpc:call(VolumecapNode, mnesia, dirty_read, [volumecap,{12,1111,Today+86400}]),    
	2 = proplists:get_value(b_update, LHStats),       % current/next
	2 = proplists:get_value(b_update, VSStats),       % current/next
	0 = IntB1#vc_interval.available,                  % current changes to 0
	true = (IntB2#vc_interval.available > 0 ),
	0 = Volumecap#volumecap.available,                % current volume cap change to 0
	true = (VolumecapTomorrow#volumecap.available>0), % current volume cap change to 0
	?DEBUG("... decrease meta total resulting no available test ... ok"),
	ok.

