-module(gpaxos).
-behaviour(gen_server).

% Client API
-export([start_link/0, create_server/0, read/1, write/1, stop/0]).

% Server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

%---------------------------------------------------------------------------------------------
% The API for Paxos Clients to communicate with the servers
%---------------------------------------------------------------------------------------------
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Add a server to the group of processes participating in the paxos cluster.
create_server() -> gen_server:cast(?MODULE, create_server).

% Read a given key from an individual server.
read(Key) -> gen_server:call(?MODULE, {read, Key}).

% Write a value to an individual server and return the new hash Key.
write(Value) -> gen_server:call(?MODULE, {write, Value}).

stop() -> gen_server:cast(?MODULE, stop).

%---------------------------------------------------------------------------------------------
% Server Implementation
%---------------------------------------------------------------------------------------------
init([]) -> 
    io:format("init~n"),
    Servers = [],
    {ok, Servers}.

handle_call({read, Key}, _From, Servers) ->    
    log("Reading Value..."),
    Replies = lists:map(fun(Server) -> gpaxos_server:read(Server, Key) end, Servers),
    %N = length(Servers),
    % Replies = collect_replies(N),
    Reply = consensus(aggregate_list(Replies)),
    {reply, Reply, Servers};
handle_call({write, Value}, _From, Servers) ->
    Server = get_random_server(Servers),
    Reply = gpaxos:write(Server, Value),
    {reply, Reply, Servers}.

handle_cast(create_server, Servers) ->
    log("Adding server. Total count:", [length(Servers)+1]),
    NewServerPid = gpaxos_server:start_link(),
    {noreply, [NewServerPid|Servers]};
handle_cast(stop, State) -> {stop, normal, State}.

handle_info(_Any, Servers) -> {noreply, Servers}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(normal, _State) -> ok.

%---------------------------------------------------------------------------------------------
% Utility functions
%---------------------------------------------------------------------------------------------
%Client logging
log(Message) -> log("CLIENT", Message, []).
log(Message, Args) -> log("CLIENT", Message, Args).
log(ProcessType, Message, Args) ->
    ArgsFormat = lists:flatten(["~p " || _ <- lists:seq(1, length(Args))]),
    io:format(" ~s ~p - ~s " ++ ArgsFormat ++ "~n", [ProcessType, self(), Message] ++ Args).

get_random_server(Servers) ->
    %Servers = get_servers(),
    Index = random:uniform(length(Servers)),
    Server = lists:nth(Index, Servers),
    % Server ! Message,
    % ok.
    Server.

consensus(Replies) -> consensus(length(Replies), Replies).

consensus(N, Replies) ->
    SortedRepliesAsc = lists:keysort(2, Replies), %We're sorting here by the count, rather than the value(key)
    SortedRepliesDesc = lists:reverse(SortedRepliesAsc),
    consensus_internal(N, SortedRepliesDesc).

consensus_internal(_N, []) ->
    consensus_not_reached;
consensus_internal(N, [{Value, Count}|T]) ->
    case Count >= (N div 2 + 1) of
        true -> {Value, Count};
        false -> consensus_internal(N, T)
    end.

aggregate_list(List) -> aggregate_list(List, dict:new()).
aggregate_list([], Dict) -> dict:to_list(Dict);
aggregate_list([Key|T], Dict) -> aggregate_list(T, dict:update_counter(Key, 1, Dict)).