-module(vc_master_sup).
-include("include/records.hrl").
-behaviour(supervisor).
-export([start_link/1, init/1]).
-compile(export_all).

start_link(_Args) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
	MasterNodes = case application:get_env(vc_master,master_nodes) of
		{ok,Nodes} -> Nodes;
		undefined -> []
	end,
	lists:foreach(fun(Node) -> net_adm:ping(Node) end, MasterNodes),
	timer:sleep(1000), % give some time for global variables to be propagated 
	Primary    = global:whereis_name( vc_master:master_name(primary) ),
	error_logger:info_msg("vc_master_sup: Primary ~p", [Primary]),
	Secondary  = global:whereis_name( vc_master:master_name(secondary) ),
	error_logger:info_msg("vc_master_sup: Secondary ~p", [Secondary]),
	MasterFor  = if 
		Primary   == undefined -> primary;
		Secondary == undefined -> secondary; 
		true -> none 
	end,
	case MasterFor of
		primary   -> 
			{ok, {{ one_for_one, 5, 60}, primary_child_specs()}};
		secondary -> 
			{ok, {{ one_for_one, 5, 60}, secondary_child_specs()}};
		_ ->
			error_logger:info_msg("vc_master: primary found, secondary found, exiting"),
			{error, primary_and_secondary_already_started}
	end.

primary_child_specs() ->
	[  % {Id,StartFunc,Restart,Shutdown,Type,Modules} 
		{ vc_master_primary,   {vc_master_primary, start_link, []},   transient, 1000, worker, [vc_master_primary]   },  
		{ vc_meta_handler,     {vc_meta_handler, start_link, []},     transient, 1000, worker, [vc_meta_handler]     }, 
		{ vc_interval_handler, {vc_interval_handler, start_link, []}, transient, 1000, worker, [vc_interval_handler] }, 
		{ vc_local_handler,    {vc_local_handler, start_link, []},    transient, 1000, worker, [vc_local_handler]    }, 
		{ vc_remote_handler,   {vc_remote_handler, start_link, []},   transient, 1000, worker, [vc_remote_handler]   } 
	].
	
secondary_child_specs() ->
	[  % {Id,StartFunc,Restart,Shutdown,Type,Modules} 
		{ vc_master_secondary, {vc_master_secondary, start_link, []}, transient, 1000, worker, [vc_master_secondary]     }   
	].
