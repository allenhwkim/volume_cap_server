#!/usr/bin/env escript
% escript $BIN_PATH/volume_cap.escript stop|status|add_node|del_node $NODENAME $TARGET_NODE
main([Command, NodeName]) ->
	try
		true = code:add_path(filename:dirname(escript:script_name()) ++ "/../ebin"),
		{ok, _} = net_kernel:start(['volumecap_admin@127.0.0.1',longnames]),
		true = erlang:set_cookie(node(), 'volumecap'),
		pong = net_adm:ping(list_to_atom(NodeName)),
		case Command of
			"start_vc" ->
				io:format("~p~n",[rpc:call(list_to_atom(NodeName), application, start, [vc_master])]);
			"stop_node" ->
				io:format("~p~n",[rpc:call(list_to_atom(NodeName), vc_master, stop, [])]),
				io:format("~p~n",[rpc:call(list_to_atom(NodeName), init, stop, [])]);
			"status" ->
				io:format("~p~n",[rpc:call(list_to_atom(NodeName), vc_master, status, [])]);
			Any ->
				io:format("Invalid command ~p~n",[Any])
		end
	catch
		_E:_R ->
			exit({_E,_R,erlang:get_stacktrace()})
	end.
