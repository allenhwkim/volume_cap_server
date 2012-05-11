%..............................
%. spawn interval test
%..............................
%   . cast spawn_interval with matching number
%   . check if matching number current interval is created
%   . check if next interval is created
%   . LH stats.b_new increases
%   . VS stats.b_new increases
%   . LH stats.topup increases
%   . interval.available decreases
%   . VS stats.topup increases
%   . volumecap.available increses
-module(spawn_interval_test).
-include_lib("../include/records.hrl").
-compile(export_all).

test() ->
	volumecap_test:setup(),
	spawn_interval_test(),
	ok.

spawn_interval_test() ->
	?DEBUG("... spawn interval test"),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),
	
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	% create new metas
	lists:foreach( fun(Num) ->
		ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta#vc_meta{id=Num}}])
	end,[0,1,2,144,145]),
	timer:sleep(500),
	% delete next intervals, so that I can spawn it
	lists:foreach( fun(Num) ->
		ok = rpc:call(MasterNode, gen_server,cast,[{global,vc_interval_handler_12},{'delete',{12,Num,Today+86400}}])
	end,[0,1,2,144,145]),
	rpc:call(MasterNode, gen_server,cast,[{global,vc_interval_handler_12},{'delete',{12,0,Today}}]), % DELETE this too
	timer:sleep(500),
	% check if interval,volumecaps are deleted
	lists:foreach( fun(Num) ->
		[] = rpc:call(MasterNode,mnesia,dirty_read, [vc_interval,{12,Num,Today+86400}]), 
		[] = rpc:call(VolumecapNode, mnesia, dirty_read, [volumecap,{12,Num,Today+86400}])
	end,[0,1,2,144,145]),
	%
	%?DEBUG("interval state ~p",[rpc:call(MasterNode, gen_server,call,[{global,vc_interval_handler_12},get_state])]),
	%?DEBUG("local state ~p",[rpc:call(MasterNode, gen_server,call,[{global,vc_local_handler_12},get_state])]),
	%?DEBUG("volumecap state ~p",[gen_server:call({volumecap_server, VolumecapNode}, get_state)]),
	% spawn interval 0, (0,144,288,....)
	ok = rpc:call(MasterNode, gen_server,cast,[{global,vc_interval_handler_12},{'spawn_intervals',0}]),
	timer:sleep(500),
	% check if current and next intervals and volumecaps are created
	lists:foreach( fun(Num) ->
		[_CurInt]  = rpc:call(MasterNode,mnesia,dirty_read, [vc_interval,{12,Num,Today}]), 
		[_CurVc]   = rpc:call(VolumecapNode, mnesia, dirty_read, [volumecap,{12,Num,Today}]),    
		[_NextInt] = rpc:call(MasterNode,mnesia,dirty_read, [vc_interval,{12,Num,Today+86400}]), 
		[_NextVc]  = rpc:call(VolumecapNode, mnesia, dirty_read, [volumecap,{12,Num,Today+86400}])    
	end,[0,144]),
	lists:foreach( fun(Num) ->
		[] = rpc:call(MasterNode,mnesia,dirty_read, [vc_interval,{12,Num,Today+86400}]), 
		[] = rpc:call(VolumecapNode, mnesia, dirty_read, [volumecap,{12,Num,Today+86400}])    
	end,[1,2,145]),
	%
	%?DEBUG("interval state ~p",[rpc:call(MasterNode, gen_server,call,[{global,vc_interval_handler_12},get_state])]),
	%?DEBUG("local state ~p",[rpc:call(MasterNode, gen_server,call,[{global,vc_local_handler_12},get_state])]),
	%?DEBUG("volumecap state ~p",[gen_server:call({volumecap_server, VolumecapNode}, get_state)]),
	?DEBUG("... spawn interval test ... ok"),
	ok.
