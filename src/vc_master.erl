-module(vc_master).
-include("include/records.hrl").
-behaviour(application).
-export([start/2, stop/1]).
-compile(export_all).

start() ->
	application:start(vc_master).

start(_Type, StartArgs) ->
	vc_master_sup:start_link(StartArgs).

stop() ->
	error_logger:info_msg("vc_master: stopping application"),
	del_node(node()),
	application:stop(vc_master).

stop(_State) ->
	ok.

restart(Handler) ->
	supervisor:terminate_child(vc_master_sup, Handler),
	supervisor:restart_child(vc_master_sup, Handler).

status(MetaId) ->
	Metas = mnesia:dirty_read(vc_meta,MetaId),
	Intervals = mnesia:dirty_index_read(vc_interval,MetaId,#vc_interval.meta_id),
	{state,AllNodes,_Stats} = gen_server:call({global,vc_master:handler_name(local)}, get_state),
	Volumecaps = lists:map( fun(Node) ->
		{Node, rpc:call(Node, mnesia, dirty_index_read, [volumecap,MetaId,#volumecap.meta_id])}
	end, AllNodes),
	[	{vc_meta,Metas},
		{vc_interval, Intervals},
		{volumecap, Volumecaps}].
	
status() ->
	PrimaryState = case global:whereis_name(vc_master:master_name(primary)) of 
		PriPid when is_pid(PriPid) -> gen_server:call(PriPid, get_state);
		_ -> undefined
	end,
	MasterNodes = case application:get_env(vc_master, master_nodes) of 
		{ok, Nodes} -> Nodes;
		undefined -> []
	end,
	SecondaryState = case global:whereis_name(vc_master:master_name(secondary)) of 
		SecPid when is_pid(SecPid) -> gen_server:call(SecPid, get_state);
		_ -> undefined
	end,
	[
		{master_nodes, MasterNodes},
		{primary_state, PrimaryState},
		{secondary_state, SecondaryState}
	].

add_node(Node) ->
	{ok, OldMasterNodes} = application:get_env(vc_master, master_nodes),
	pong = net_adm:ping(Node),
	NewMasterNodes = lists:usort(OldMasterNodes ++ [Node]),
	lists:foreach( fun(MasterNode) ->
		ok = rpc:call( MasterNode, application, set_env, [vc_master, master_nodes, NewMasterNodes]) 
	end, OldMasterNodes),
	error_logger:info_msg("vc_master: A new volume cap node(~p) is added, resulting in nodes; ~p",[Node, NewMasterNodes]),
	NewMasterNodes.

del_node(Node) ->
	{ok, OldMasterNodes} = application:get_env(vc_master, master_nodes),
	NewMasterNodes = lists:usort(OldMasterNodes -- [Node]),
	lists:foreach( fun(MasterNode) ->
		catch rpc:call( MasterNode, application, set_env, [vc_master, master_nodes, NewMasterNodes]) 
	end, OldMasterNodes),
	error_logger:info_msg("vc_master: A volume cap node(~p) is deleted, resulting in nodes; ~p",[Node, NewMasterNodes]),
	NewMasterNodes.

master_name(Type) ->
	DcId = case application:get_env(vc_master, dc_id) of
		{ok, Id} -> Id;
		undefined -> 12 
	end,
	list_to_atom("vc_master_"++atom_to_list(Type)++"_"++integer_to_list(DcId)).

handler_name(Type) ->
	DcId = case application:get_env(vc_master, dc_id) of
		{ok, Id} -> Id;
		undefined -> 12 
	end,
	list_to_atom("vc_"++atom_to_list(Type)++"_handler_"++integer_to_list(DcId)).

	
wait_for_primary_vc_master_ready(Count) -> 
	PrimaryVcMaster = vc_master:master_name(primary),
	try
		gen_server:call({global,PrimaryVcMaster},get_state) 
	catch Class:Error ->
		error_logger:error_report([{class, Class},{error,Error}]),
		if 
			Count > 10 ->
				error_logger:error_msg("vc_master: ~p is not ready.",[PrimaryVcMaster]),
				{error, primary_vc_master_is_not_ready};
			true ->
				error_logger:info_msg("vc_master: ~p is not ready. counting ~p",[PrimaryVcMaster, Count]),
				timer:sleep(1000),
				wait_for_primary_vc_master_ready(Count+1) 
		end
	end,
	ok.

wait_for_tables(TableList) ->
	case mnesia:wait_for_tables(TableList, 1000) of
		ok    -> 
			ok;
		_Error -> 
			error_logger:error_msg("vc_master: Waiting for tables ~p failed ~n",[TableList]),
			error_logger:error_msg("vc_master: mnesia:schema(vc_meta) ~p~n",[mnesia:schema(vc_meta)]),
			error_logger:error_msg("vc_master: mnesia:schema(vc_interval) ~p~n",[mnesia:schema(vc_interval)]),
			wait_for_tables(TableList)
	end.

create_tables_if_not_exists(Tables) ->
	try
		lists:foreach( fun(Table) ->
			mnesia:table_info(Table, disc_copies)  % this will cause exception if table not exists
		end, Tables)
	catch _:_ ->
		try 
			[_Node] = mnesia:table_info(schema,disc_copies)  % this will cause exception if schema is not created yet
		catch _:_ ->
			error_logger:info_msg("vc_master: Result mnesia:stop ~p~n", [mnesia:stop()]),
			error_logger:info_msg("vc_master: Result mnesia:delete_schema ~p~n", [mnesia:delete_schema([node()])]),
			error_logger:info_msg("vc_master: Result mnesia:create_schema ~p~n", [mnesia:create_schema([node()])]),
			error_logger:info_msg("vc_master: Result mnesia:start ~p~n", [mnesia:start()])
		end,
		lists:foreach( fun(Table) ->
			TabDef= case Table of
				vc_meta     -> [{disc_copies,[node()]},{type,set},{attributes, record_info(fields,vc_meta)}];
				vc_interval -> [{disc_copies,[node()]},{type,set},{attributes, record_info(fields,vc_interval)},{index,[meta_id]}]
			end,
			error_logger:info_msg("vc_table: Creating tables Table ~p TabDef~p", [Table, TabDef]),
			CreateTableResult = mnesia:create_table(Table, TabDef),
			error_logger:info_msg("vc_master: Creating tables Result ~p", [CreateTableResult])
		end, Tables)
	end.

