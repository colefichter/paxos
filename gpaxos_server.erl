-module(gpaxos_server).
-behaviour(gen_server).

% Client API
-export([start_link/0, read/2, write/2]).

% Server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

%---------------------------------------------------------------------------------------------
% The API for Paxos Clients to communicate with the servers
%---------------------------------------------------------------------------------------------
start_link() ->
    {ok, Pid} = gen_server:start_link(?MODULE, [], []),
    Pid.

% Read a given key from an individual server.
read(ServerPid, Key) -> gen_server:call(ServerPid, {read, Key}).

% Write a value to an individual server and return the new hash Key.
write(ServerPid, Value) -> gen_server:call(ServerPid, {write, Value}).

%---------------------------------------------------------------------------------------------
% Server Implementation
%---------------------------------------------------------------------------------------------

init([]) -> 
    log("Starting server"),
    Dict = dict:new(), % We should start off not knowing any value
    Proposals = [],
    {ok, {Dict, Proposals}}.

handle_call({read, Key}, _From, State) ->
    io:format("get ~p~n", [State]),
    {reply, {current_value, State}, State};
handle_call({write, Value}, _From, State) ->
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

%---------------------------------------------------------------------------------------------
% Utility functions
%---------------------------------------------------------------------------------------------
log(Message) -> log("SERVER", Message, []).
log(Message, Args) -> log("SERVER", Message, Args).
log(ProcessType, Message, Args) ->
    ArgsFormat = lists:flatten(["~p " || _ <- lists:seq(1, length(Args))]),
    io:format(" ~s ~p - ~s " ++ ArgsFormat ++ "~n", [ProcessType, self(), Message] ++ Args).