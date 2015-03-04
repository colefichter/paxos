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
	message_random_server({write, Value, self()}).

%---------------------------------------------------------------------------------------------
% The API for the Paxos Servers
%---------------------------------------------------------------------------------------------
server_loop([]) -> %Init routine for the server loop. Cleaner than defining defaults in create_server().
	HighestSeq = 0, % Ref > 0 for ANY reference, Ref. This means we don't have to code lots of cases with a pattern like no_reference_yet...
	Value = nothing, % We should start off not knowing any value
	Proposals = [],
	server_loop({Value, HighestSeq, Proposals});
server_loop(State) ->
	receive
		{read, Client} ->
			Client ! State,
			server_loop(State);
		{write, Value, From} ->
			% Initiate a paxos consensus to adopt Value as the new stored value.
			% Step 1 in Paxos: The server that handles the client's write request begins a proposal to the other servers.
			{Seq, NumMessages} = send_prepare(Value),
			Replies = collect_replies(NumMessages, Seq),
			% Did we get enough replies? (Only the positive ones are collected)
			% Normal majority = (N/2) + 1. But this current server counts as 1 (and was not messaged), so we only need N/2 or more:
			case length(Replies) >= ((NumMessages+1) div 2) of
				true ->
					not_implemented;
				false ->
					not_implemented
			end,
			server_loop(State);
		{prepare, Seq, Value, From} ->
			% Step 2: When a server receives a prepare message, it must check if that prepare is the highest ever seen
			{Value, HighestSeq, Proposals} = State,
			case Seq >= HighestSeq of
				true ->
					% PROMISE to accept nothing lower than Seq. (Must include prev. promise and value).
					% We're supposed to send the old value (if any), but is that really needed?
					%From ! {promise, Seq, self(), {prev, HighestSeq, Value}},
					From ! {propose, Seq, accept},
					server_loop({Value, Seq, Proposals});
				false ->
					% We've already promised to accept a higher sequence number... ignore this proposal
					From ! {propose, Seq, reject},
					server_loop(State)
			end;
		{stop} -> ok
	end.

% 
send_prepare(Value) ->
	Seq = make_ref(), % Refs are great sequence numbers because the increase monotonically, are unique, and can be compared.
	Message = {prepare, Seq, Value, self()},
	N = message_all_servers(Message, self()), % self() excludes this server from sending a message to itself.
	{Seq, N}.

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
	RootPid = spawn(?MODULE, root_loop, [[]]), %The empty list will invoke an init routine, above.
	register(root, RootPid),
	RootPid.

root_loop(Servers) ->
	io:format("   started! Servers: ~p~n", [Servers]),
	receive 
		{create_server} ->
			io:format("Spawning server..."),
			NewServer = spawn(?MODULE, server_loop, [[]]),
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
message_random_server(Message) ->
	Servers = get_servers(),
	Index = random:uniform(length(Servers)),
	Server = lists:nth(Index, Servers),
	Server ! Message,
	ok.

% Send a meesage to EVERY server in the cluster.
message_all_servers(Message) ->
	Servers = get_servers(),
	message_all_servers(Servers).
	
% Send a message to all servers EXCEPT ExcludeMe (so the server doesn't propose a value to itself)
message_all_servers(Message, ExcludeMe) when is_pid(ExcludeMe) ->
	Servers = lists:filter(fun(X) -> X /= ExcludeMe end, get_servers()),
	message_all_servers(Servers);
message_all_servers(Message, Servers) when is_list(Servers) ->
	lists:foreach(fun(Server) -> Server ! Message end, Servers),
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

collect_replies(N) ->
	collect_replies(N, nothing).

collect_replies(N, ThingToMatchAgainst) ->
	collect_replies(N, ThingToMatchAgainst, []).

collect_replies(0, ThingToMatchAgainst, Replies) ->
    Replies;
collect_replies(N, ThingToMatchAgainst, Replies) ->
	% I don't want to write the same collecting code a bunch of times, so this should work for different situations, but
	% it might not be a great idea. Let's see what happens...
    receive
		% Client is waiting for responses to a read request
		{read_value, _Server, Value} ->	collect_replies(N-1, ThingToMatchAgainst, [Value|Replies]);
		% Server is waiting for responses to a propose request (two lines):
		{propose, ThingToMatchAgainst, accept} -> collect_replies(N-1, ThingToMatchAgainst, [ThingToMatchAgainst | Replies]);
		{propose, ThingToMatchAgainst, reject} -> collect_replies(N-1, ThingToMatchAgainst, Replies)
    end.
% TODO: INTRODUCE A TIMEOUT SOMEWHERE IN HERE? ^^^



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