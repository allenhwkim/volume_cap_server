%..............................
%. delete meta test
%..............................
%    . meta is deleted
%    . all intervals are deleted
%    . LH stats.b_del is increased
%
%    . VS stats.b_del is increased
%    . volumecap is deleted
-module(delete_meta_test).
-include_lib("../include/records.hrl").
-compile(export_all).

% Eunit has too much timeout limit, therefore we made our own test.
test() ->
	volumecap_test:setup(),
	delete_meta_test(),
	ok.

delete_meta_test() -> 
	?DEBUG("... delete meta test"),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'delete',1111}]), % non-existing one
	timer:sleep(100),
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'delete',1111}]), % existing one
	timer:sleep(100),
	% intervals are deleted
	[] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today+86400}]),
	% stats are correct
	{state,IHStats} = rpc:call(MasterNode, gen_server,call,[{global,vc_interval_handler_12},get_state]),
	2 = proplists:get_value(del_all, IHStats), % non-existing one/existing one
	{state,_MasterNodes,LHStats} = rpc:call(MasterNode, gen_server,call,[{global,vc_local_handler_12},get_state]),
	2 = proplists:get_value(b_delete, LHStats),
	% volumecaps are deleted
	[] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today}]),     
	[] = rpc:call(MasterNode,mnesia,dirty_read,[vc_interval,{12,1111,Today+86400}]),
	% stats are correct
	{state,_,VSStats}  = gen_server:call({volumecap_server, VolumecapNode}, get_state),
	2 = proplists:get_value(b_delete, VSStats),       % current/next
	?DEBUG("... delete meta test ... ok"),
	ok.


