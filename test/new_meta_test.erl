%..............................
%. new meta test
%..............................
-module(new_meta_test).
-include_lib("../include/records.hrl").
-compile(export_all).

test() ->
	volumecap_test:setup(),
	new_meta_with_current_date_test(),
	new_meta_with_current_date_with_initial_total_test(),
	new_meta_with_future_date_test(),
	ok.

%    . new meta with current date
new_meta_with_current_date_test() ->
	?DEBUG("... new meta with current date test"),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),
	
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(500),
	[NewMeta] = rpc:call(MasterNode,mnesia,dirty_read, [vc_meta,1111]),                         % new meta is created
	[_] = rpc:call(MasterNode,mnesia,dirty_read, [vc_interval,{12,1111,Today}]),                % new intervals are created
	[_] = rpc:call(MasterNode,mnesia,dirty_read, [vc_interval,{12,1111,Today+86400}]),% 
	{state,IHStats} = rpc:call(MasterNode, gen_server,call,[{global,vc_interval_handler_12},get_state]),
	1 = proplists:get_value(add_new, IHStats),                                                  % IH stats.add_new increaes
	{state,_Nodes,LHStats} = rpc:call(MasterNode, gen_server,call,[{global,vc_local_handler_12},get_state]),
	2 = proplists:get_value(b_new, LHStats),                                                    % LH stats.b_new increaes
	{state,_LH,VSStats} = gen_server:call({volumecap_server, VolumecapNode}, get_state),
	2 = proplists:get_value(b_new, VSStats),                                                    % VS stats.b_new increaes
	2 = proplists:get_value(topup, LHStats),                                                    % LH stats.topup increaes
	[VcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{12,1111,Today}]),    
	450.00 = VcInterval#vc_interval.available,                                                  % interval.available decreases
	2 = proplists:get_value(topup, VSStats),                                                    % VS stats.topup increaes
	[Volumecap] = rpc:call(VolumecapNode, mnesia, dirty_read, [volumecap,{12,1111,Today}]),    
	50.00 = Volumecap#volumecap.available,                                                      % volumecap.available increases
	?DEBUG("... new meta with current date test ... ok"),
	ok.

%    . new meta with current date with initial total
new_meta_with_current_date_with_initial_total_test() ->
	?DEBUG("... new meta with current date test with initial total"),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),
	
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta,500}]),
	timer:sleep(500),
	[NewMeta] = rpc:call(MasterNode,mnesia,dirty_read, [vc_meta,1111]),                         % new meta is created
	[IntA1] = rpc:call(MasterNode,mnesia,dirty_read, [vc_interval,{12,1111,Today}]),            % new intervals are created
	[IntA2] = rpc:call(MasterNode,mnesia,dirty_read, [vc_interval,{12,1111,Today+86400}]),% 
	250.00 = IntA1#vc_interval.total,
	500.00 = IntA2#vc_interval.total,
	{state,IHStats} = rpc:call(MasterNode, gen_server,call,[{global,vc_interval_handler_12},get_state]),
	{state,_Nodes,LHStats} = rpc:call(MasterNode, gen_server,call,[{global,vc_local_handler_12},get_state]),
	{state,_LH,VSStats} = gen_server:call({volumecap_server, VolumecapNode}, get_state),
	[Volumecap] = rpc:call(VolumecapNode, mnesia, dirty_read, [volumecap,{12,1111,Today}]),    
	1 = proplists:get_value(add_new, IHStats),
	1 = proplists:get_value(updt_initial, IHStats),
	2 = proplists:get_value(b_new, LHStats), % current/next
	1 = proplists:get_value(b_update, LHStats), % current
	2 = proplists:get_value(b_new, VSStats), % current/next
	1 = proplists:get_value(b_update, VSStats), % current
	2 = proplists:get_value(topup, LHStats), % current/next
	50.00 = Volumecap#volumecap.available,
	ok.
%    . new meta with future date 
new_meta_with_future_date_test() ->
	?DEBUG("... new meta with future date test"),
	{MasterNode, _VolumecapNode} = volumecap_test:reset(),

	{{Y,M,D},_} = calendar:local_time(),
	Tomorrow = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}})+86400,
	NewMeta = #vc_meta{'id'=1111,'start'=Tomorrow,'end'=Tomorrow+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(500),
	[NewMeta] = rpc:call(MasterNode,mnesia,dirty_read, [vc_meta,1111]),                      % new meta is created
	[] = rpc:call(MasterNode,mnesia,dirty_read, [vc_interval,{12,1111,Tomorrow}]),           % new intervals are NOT created
	[] = rpc:call(MasterNode,mnesia,dirty_read, [vc_interval,{12,1111,Tomorrow+86400}]),     % new intervals are NOT created
	?DEBUG("... new meta with future date test ... ok"),
	ok.
