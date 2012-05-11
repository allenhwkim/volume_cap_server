{application, vc_master,
 [
  {description, "Volume Cap Master"},
  {vsn, "2.0"},
  {modules,      [ 	vc_interval_handler,
					vc_local_handler,    % local request processor
					vc_master,           % vc_master application
					vc_master_primary,   % primary master
					vc_master_restarter, % used when seconcary restarts as the primary
					vc_master_secondary, % secondary master
					vc_master_sup,       % vc master supervisor
					vc_meta_handler,
					vc_remote_handler ]},
  {registered,   []},
  {applications, [kernel, stdlib, sasl, mnesia]},
  {mod, {vc_master, []}},               % vc_master:start() 
  {env, [
		  {dc_id, 12},                  % data center id of this node
		  {dc_ids, [12,22]},            % all data center ids, used to send remote request to rabbitmq
		  {remote_queues, [             % rabbitmq definition to communicate to remote data centers
				[{host,"localhost"}, {vhost,<<"/prod_12">>}, {queue,<<"vc_remote_12">>}, {exchange,<<"vc_remote">>}],
				[{host,"localhost"}, {vhost,<<"/prod_12">>}, {queue,<<"vc_remote_12">>}, {exchange,<<"vc_remote">>}]  ]},
		  {master_nodes, [              % possible master nodes, only two nodes will be used as the pri. and the sec.
			  	'node1@127.0.0.1','node2@127.0.01','node3@127.0.0.1','node3@127.0.0.1'] },  
		  {threshold_pct, 10}           % if reaches to this, we send topup request to other remote d.c.
	]}
 ]
}.
