-module(paxos).

-compile([export_all]).


%---------------------------------------------------------------------------------------------
% The API for Paxos Clients to communicate with the servers
%---------------------------------------------------------------------------------------------

% Ask all servers for the current value, then consider the majority value to be the 'true' value.
% If there is no majority, or not enough clients respond, the read fails.
read() ->
	N = message_all_servers({read, self()}),
	Replies = collect_replies(N),
	consensus(N, aggregate_list(Replies)).

% Ask a random server to store a new value. Paxos makes a guarantee that clients can send their 
% write requests to any member of the Paxos cluster, so for the demos here the client picks one 
% of the processes at random. The write request can either succeed or fail.
write(Value) ->
	not_implemented.

message_all_servers(Message) ->
	Servers = get_servers(),
	lists:foreach(fun(Server) -> Server ! Message end, Servers),
	% TODO: Collect and aggregate the replies, then return a datastructure that summarizes the results
	not_implemented,
	length(Servers). % Return the number of servers messaged (needed for collecting replies)

consensus(Replies) ->
	consensus(length(Replies), Replies).

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

%---------------------------------------------------------------------------------------------
% The API for the Paxos Servers
%---------------------------------------------------------------------------------------------
server_loop(State) ->
	receive
		{read, Client} ->
			Client ! State,
			server_loop(State);
		{todo, build_me} -> server_loop(State);
		{stop} -> ok
	end.

%---------------------------------------------------------------------------------------------
% ROOT Stuff. This process will simply keep track of the servers
%  so that we can easily look them up when we need to message them.
%---------------------------------------------------------------------------------------------

%Internal client API for Paxos clients to request the list of server processes:
get_servers() ->
	root ! {get_servers, self()},
	receive
		{servers, Servers} -> Servers
	end.

%Internal client API to add a new server:
create_server() ->
	root ! {create_server},
	ok.

init_root() ->
	io:format("Starting the root process..."),
	RootPid = spawn(?MODULE, root_loop, [[]]), %Start with zero servers.
	register(root, RootPid),
	RootPid.

root_loop(Servers) ->
	io:format("   started! Servers: ~p~n", [Servers]),
	receive 
		{create_server} ->
			io:format("Spawning server..."),
			NewServer = spawn(?MODULE, server_loop, [empty]),
			io:format("   success."),
			root_loop([NewServer|Servers]);
		{get_servers, From} ->
			io:format("Server lookup requested."),
			From ! {servers, Servers},
			root_loop(Servers);
		{stop} ->
			ok
	end.


%---------------------------------------------------------------------------------------------
% Utility functions
%---------------------------------------------------------------------------------------------
collect_replies(N) ->
	collect_replies(N, []).

collect_replies(0, Replies) ->
    Replies;
collect_replies(N, Replies) ->
    receive
		{read_value, _Server, Value} ->	   
		    collect_replies(N+1, [Value|Replies])
    end.

aggregate_list(List) ->
	aggregate_list(List, dict:new()).

aggregate_list([], Dict) ->
	dict:to_list(Dict);
aggregate_list([Key|T], Dict) ->
	aggregate_list(T, dict:update_counter(Key, 1, Dict)).

%---------------------------------------------------------------------------------------------
% Unit tests
%---------------------------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

init_root_test() ->
	Pid = init_root(),
	WhereIsIt = whereis(root),
	?assertEqual(Pid, WhereIsIt),
	Servers = get_servers(),
	?assertEqual([], Servers),
	create_server(),
	Servers2 = get_servers(),
	?assertEqual(1, length(Servers2)),
	root ! {stop}.		

aggregate_list_test() ->
	Replies = [a, b, a, c, a, d, a, b, a, b],
	Agg = aggregate_list(Replies),
	?assertEqual({a, 5}, lists:keyfind(a, 1, Agg)),
	?assertEqual({b, 3}, lists:keyfind(b, 1, Agg)),
	?assertEqual({c, 1}, lists:keyfind(c, 1, Agg)),
	?assertEqual({d, 1}, lists:keyfind(d, 1, Agg)).

consensus_found_test() ->
	Replies = [a, b, a, c, a, d, a, b, a, b],
	Agg = aggregate_list(Replies),
	?assertEqual({a,5}, consensus(Agg)).

consensus_failed_test() ->
	Replies = [a, a, b, c, d, e, f, g, h, i, j, k],
	Agg = aggregate_list(Replies),
	?assertEqual(consensus_not_reached, consensus(Agg)).	