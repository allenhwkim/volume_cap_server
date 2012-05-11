%..............................
%. volumecap topup test
%..............................
-module(volumecap_topup_test).
-include_lib("../include/records.hrl").
-include_lib("../amqp_client-2.6.1/include/amqp_client.hrl").
-compile(export_all).

% Eunit has too much timeout limit, therefore we made our own test.
test() ->
	volumecap_test:setup(),
	topup_from_local_handler_when_available_test(),
	topup_from_local_handler_when_available_is_0_test(),
	topup_from_remote_handler(),
	ok.

topup_from_local_handler_when_available_test() ->
	?DEBUG("... topup_from_local_handler_when_available_test"),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),
	% create a new cap
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	% consume 90% of available
	{ok, 5.00} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',1111, 50.00*0.9}),
	% topup request / topup response happens
	timer:sleep(100),
	{ok, 55.00} = gen_server:call({volumecap_server, VolumecapNode}, {'query',1111}),
	[VcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{12,1111,Today}]),    
	400.00 = VcInterval#vc_interval.available,  % decreased from 450.00 to 400.00
	100.00 = VcInterval#vc_interval.consumed,   % increased from 50.00 to 100.00
	[Volumecap] = rpc:call(VolumecapNode, mnesia, dirty_read, [volumecap,{12,1111,Today}]),    
	55.00 = Volumecap#volumecap.available,      % increases from 5.00 to 55.00
	?DEBUG("... topup_from_local_handler_when_available_test ... ok").

topup_from_local_handler_when_available_is_0_test() ->
	?DEBUG("... topup_from_local_handler_when_available_is_0_test"),
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),
	% create a new cap
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	% change available to 0
	[VcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{12,1111,Today}]),    
	ok = rpc:call(MasterNode, mnesia, dirty_write, [VcInterval#vc_interval{available=0,consumed=500}]),  
	timer:sleep(100),
	% spend 90% of available, to initiate topup
	{ok, 5.00} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',1111, 50.00*0.9}),
	timer:sleep(100),
	[NewVcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{12,1111,Today}]),    
	true = (NewVcInterval#vc_interval.available == 0),  % still 0, nothing changes
	[Volumecap] = rpc:call(VolumecapNode, mnesia, dirty_read, [volumecap,{12,1111,Today}]),    
	5.00 = Volumecap#volumecap.available,        % still 5, nothing changes
	{ok, 0.00} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',1111, 5}),
	timer:sleep(100),
	{ok, 0.00} = gen_server:call({volumecap_server, VolumecapNode}, {'query',1111}), % no topup happens
	?DEBUG("... topup_from_local_handler_when_available_is_0_test ... ok").

%        . call consume to volumecap server
%        . volumecap.available decreases
%        . LH stats.topup increases
%        . interval.available does not changes
%        . RH stats.topup_req increases
topup_from_remote_handler() ->
	?DEBUG("... topup_from_remote_handler_test"),
	{{Y,M,D},_} = calendar:local_time(),
	Today = calendar:datetime_to_gregorian_seconds({{Y,M,D},{0,0,0}}),
	{MasterNode, VolumecapNode} = volumecap_test:reset(),
	{_,_Dc,_Conn,MQChannel,_Ex,_Stats} = rpc:call(MasterNode,gen_server, call, [{global,vc_remote_handler_12},'get_state']),
	% create a new cap
	NewMeta = #vc_meta{'id'=1111,'start'=Today,'end'=Today+86400*10,'interval'=day,'total'=1000},
	ok = rpc:call(MasterNode, gen_server,call,[{global,vc_meta_handler_12},{'update',NewMeta}]),
	timer:sleep(100),
	% change available to 50, to cause remote topup request
	[VcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{12,1111,Today}]),    
	ok = rpc:call(MasterNode, mnesia, dirty_write, [VcInterval#vc_interval{available=50,consumed=450}]), % change available to 50
	% receive remote interval update with available 450.00
	RemoteVcInterval = #vc_interval{'id'={22,1111,Today},'dc_id'=22,'meta_id'=1111,'start'=Today,'end'=Today+86400*10,'total'=500.00,'available'=450.00,'consumed'=50.00},
	ok = rpc:call(MasterNode, mnesia, dirty_write, [RemoteVcInterval]),    
	
	% now we have availables 50.00 on local on 450.00 on remote.
	% spend 90% of available, to initiate topup
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_22">>}]),
	{ok, 5.00} = gen_server:call({volumecap_server, VolumecapNode}, {'consume',1111, 50.00*0.9}),
	timer:sleep(100),

	% when topup processes, changes interval.available fron 50.00 -> 0.00, and volumecap.available from 5.00 to 55.00.
	% the above action also triggers sending topup request to remote handler
	[LocalVcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{12,1111,Today}]),    
	[RemoteVcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{22,1111,Today}]),    
	{ok, 55.00} = gen_server:call({volumecap_server, VolumecapNode}, {'query',1111}),
	0.00   = LocalVcInterval#vc_interval.available,
	500.00 = LocalVcInterval#vc_interval.consumed,     

	% lets's see if we send topup request to vc_remote_22
	{#'basic.get_ok'{}, #amqp_msg{payload=Payload1}}= rpc:call(MasterNode, amqp_channel,call, [MQChannel, #'basic.get'{queue= <<"vc_remote_22">>, no_ack=true}]),
	{'write',_,_} = binary_to_term(Payload1),  % sending interval update from consume to remote d.c. 22 
	{#'basic.get_ok'{}, #amqp_msg{payload=Payload2}}= rpc:call(MasterNode, amqp_channel,call, [MQChannel, #'basic.get'{queue= <<"vc_remote_22">>, no_ack=true}]),
	{'topup',LocalVcInterval,50.00} = binary_to_term(Payload2),

	% then we send topup response with amount 50.00 from remote d.c. 22 to local d.c. 12
	#'queue.purge_ok'{} = rpc:call(MasterNode, amqp_channel, call, [MQChannel, #'queue.purge'{queue= <<"vc_remote_12">>}]),
	ok = rpc:call(MasterNode, amqp_channel, cast, [ MQChannel, #'basic.publish'{exchange= <<"vc_remote">>, routing_key= <<"12.vc_remote">>},
			#amqp_msg{props=#'P_basic'{}, payload=erlang:term_to_binary({'topup',LocalVcInterval,50.00})} ]),
	timer:sleep(100),

	% remote handler consumer reads top up message, and add it to vc_interval
	[NewLocalVcInterval] = rpc:call(MasterNode, mnesia, dirty_read, [vc_interval,{12,1111,Today}]),    
	550.00 = NewLocalVcInterval#vc_interval.total,        % 5000.00 + 50.00
	50.00  = NewLocalVcInterval#vc_interval.available,    % 0.00 + 50.00
	500.00 = NewLocalVcInterval#vc_interval.consumed,    
	
	?DEBUG("... topup_from_remote_handler_test ... ok").

