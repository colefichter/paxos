-module(test).
-behaviour(gen_server).

-compile([export_all]).


start_link() ->
    {ok, Pid} = gen_server:start_link(?MODULE, [], []),
    Pid.

get(Pid) -> gen_server:call(Pid, get).

set(Pid, Value) -> gen_server:cast(Pid, {set, Value}).




init([]) -> 
    io:format("init~n"),
    {ok, no_state}.

handle_call(get, _From, State) ->
    io:format("get ~p~n", [State]),
    {reply, {current_value, State}, State}.

handle_cast({set, Value}, State) ->
    io:format("set ~p to ~p~n", [State, Value]),
    {noreply, Value}.

handle_info(_Any, State) -> 
    io:format("unexpected message~n"),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) -> 
    io:format("code change~n"),
    {ok, State}.

terminate(reason, _State) -> 
    io:format("stopping"),
    ok.