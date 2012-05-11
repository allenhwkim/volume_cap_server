-module(vc_master_restarter).
-behaviour(gen_server).
-export([start/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {} ).

start() ->
	gen_server:start({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
	{ok, #state{}}.

handle_cast({restart,Appl}, State) -> 
	error_logger:info_msg("vc_master_restarter: received restart of ~p",[Appl]),
	timer:sleep(500),
	application:stop(Appl),
	timer:sleep(1000),
	application:start(Appl),
	{noreply, State};

handle_cast(_Any, State) -> {noreply, State}.
handle_call(_Any, _From, State) -> {reply, ignored, State}.
handle_info(_Any, State) -> {noreply, State}.
terminate(_OtherReason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
