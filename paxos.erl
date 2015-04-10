-module(paxos).

-compile([export_all]).

%% This version creates multiple process which each replicate a single erlang term. The paxos consensus
%% algorithm is used to agree on writes/updates to the current value.
%% See pkv.erl for a hash table version.

start() ->
	_RootPid = init_root(),
	[create_server() || _ <- lists:seq(1,3)],
	io:format("~n*** Running with 3 servers.~n~n"),
	read(),
	% For unit tests... pause the test thread for a moment to allow message passing to complete.
	timer:sleep(100).

stop() ->
	{stop} = root ! {stop}.

%---------------------------------------------------------------------------------------------
% The API for Paxos Clients to communicate with the servers
%---------------------------------------------------------------------------------------------

% Ask all servers for the current value, then consider the majority value to be the 'true' value.
% If there is no majority, or not enough clients respond, the read fails.
read() ->
	log_c("Reading Value..."),
	N = message_all_servers({read, self()}),
	Replies = collect_replies(N),
	consensus(N, aggregate_list(Replies)).

% Ask a random server to store a new value. Paxos makes a guarantee that clients can send their 
% write requests to any member of the Paxos cluster, so for the demos here the client picks one 
% of the processes at random. The write request can either succeed or fail.
write(Value) ->
	log_c("Writing value..."),
	message_random_server({write, Value, self()}).

%---------------------------------------------------------------------------------------------
% The API for the Paxos Servers
%---------------------------------------------------------------------------------------------
server_loop([]) -> %Init routine for the server loop. Cleaner than defining defaults in create_server().
	%HighestSeq = 0, % Ref > 0 for ANY reference, Ref. This means we don't have to code lots of cases with a pattern like no_reference_yet...
	CurrentValue = nothing, % We should start off not knowing any value
	Proposals = [],
	server_loop({CurrentValue, Proposals});
server_loop({CurrentValue, Proposals}) ->
	NewState = receive
		{read, Client} ->
			log("replying to read request:", [CurrentValue]),
			Client ! {read_value, self(), CurrentValue},
			{CurrentValue, Proposals};
		{write, WriteValue, _From} ->
			log("initiating write request:", [WriteValue]),
			handle_write_request(WriteValue),
			{CurrentValue, Proposals};
		{prepare, ProposedSeq, ProposedValue, From} ->
			log("Responding to", [prepare, ProposedSeq, ProposedValue, From]),
			% Step 2: When a server receives a prepare message, it must check if that prepare is the highest ever seen			
			{HighestSeq, HighestSeqValue} = highest_proposal(Proposals),			
			case ProposedSeq > HighestSeq of
				true ->
					% PROMISE to accept nothing lower than Seq. (Must include prev. promise and value, if any. Will be {0, nothing} if no other proposals exist).
					From ! {promise, ProposedSeq, accept, {HighestSeq, HighestSeqValue}},
					{CurrentValue, [{ProposedSeq, ProposedValue}|Proposals]}; %Add this promise to our list of proposals.
				false ->
					% We've already promised to accept a higher sequence number... ignore this proposal by sending a reject message
					From ! {promise, ProposedSeq, reject}, % Sending this makes the system faster as it won't have to wait for a timeout.
					{CurrentValue, Proposals}
			end;
		{accept, AcceptSeq, _AcceptValue, From} ->
			log("Responding to", [accept, AcceptSeq, _AcceptValue, From]),
			{HighestSeq, _HighestSeqValue} = highest_proposal(Proposals),			
			case AcceptSeq >= HighestSeq of 
				true ->					
					% This is the most recent proposal, so we're all good to accept it.
					log("  Accept acked."),
					From ! {accept_ack, AcceptSeq, accept},
					{CurrentValue, Proposals};
				false ->
					% We've already issued a promise to a proposal with a higher value than this one! Ignore.
					log("  Accept rejected."),
					From ! {accept_ack, AcceptSeq, reject},
					{CurrentValue, Proposals}
			end;
		{decision, NewValue} ->
			log("Decision made!", [NewValue]),
			% This round of Paxos has chosen a value! We can finally update the local state and reset the proposals.
			{NewValue, []};
		{set_state, MandatedValue, MandadedProposals} ->
			% For testing and debugging, this allows us to manually adjust a server's state.
			{MandatedValue, MandadedProposals};
		{stop} -> ok;
		AnythingElse -> log("Unhandled message", [AnythingElse])
	end,
	server_loop(NewState).

handle_write_request(WriteValue) ->
	% Initiate a paxos consensus to adopt Value as the new stored value.
	% Step 1 in Paxos: The server that handles the client's write request begins a proposal to the other servers.
	{Seq, NumMessages} = send_prepare(WriteValue),
	Replies = collect_replies(NumMessages, Seq),
	log("Write request received replies", [Replies]),
	% Did we get enough replies? (Only the positive ones are collected)
	% Normal majority = (N/2) + 1. But this current server counts as 1 (and was not messaged), so we only need N/2 or more:
	case length(Replies) >= ((NumMessages+1) div 2) of
		true ->			
			% Step 3: We got enough replies. Now we tell all servers to accept the value!			
			% Most of these reply seq values should be Zero, but if there is one that is not zero, replace our WriteValue with that value
			{HighestSeq, HighestSeqValue} = highest_proposal(Replies),			
			case HighestSeq > 0 of				
				true -> 
					log("Moving to accept", [HighestSeqValue]),
					handle_accept_phase(Seq, HighestSeqValue);
				false -> 
					log("Moving to accept", [WriteValue]),
					handle_accept_phase(Seq, WriteValue)
			end;
		false ->
			% Not enough replies. Abandon the write request.
			not_implemented
	end.

handle_accept_phase(Seq, Value) ->
	N = send_accept(Seq, Value),
	Replies = collect_replies(N, Seq),
	log("Accept reqeust received replies", [Replies]),
	case length(Replies) >= ((N+1) div 2) of
		true -> 
			% We have consensus! Decision is final. Update all servers (including myself, because it's easier than trying to return the new value)
			send_decision(Value);
		false ->
			% Not enough peers accepted... abort.
			not_implemented
	end.

send_decision(Value) ->
	message_all_servers({decision, Value}).

% Return the {Seq, Value} tuple with the highest
highest_proposal([]) ->
	{0, nothing}; % Zero is a good default sequence number because 0 < Ref() for ALL references created with make_ref().
highest_proposal(Proposals) ->
	Sequences = lists:map(fun({Seq, _Value}) -> Seq end, Proposals),
	MaxSeq = lists:max(Sequences),
	lists:keyfind(MaxSeq, 1, Proposals).

% Send a prepare request to all of the OTHER servers (ie. don't send the request to myself).
send_prepare(Value) ->
	Seq = make_ref(), % Refs are great sequence numbers because the increase monotonically, are unique, and can be compared.
	Message = {prepare, Seq, Value, self()},
	N = message_all_servers(Message, self()), % self() excludes this server from sending a message to itself.
	{Seq, N}.

send_accept(Seq, Value) ->
	Message = {accept, Seq, Value, self()},
	N = message_all_servers(Message, self()), % self() excludes this server from sending a message to itself.
	N.

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
	io:format("Starting the root process...~n"),
	RootPid = spawn(?MODULE, root_loop, [[]]), %The empty list will invoke an init routine, above.
	register(root, RootPid),
	RootPid.

root_loop(Servers) ->
	receive 
		{create_server} ->
			NewServer = spawn_link(?MODULE, server_loop, [[]]),
			root_loop([NewServer|Servers]);
		{get_servers, From} ->
			From ! {servers, Servers},
			root_loop(Servers);
		{stop} ->
			exit(shutdown) % This will shutdown the servers too.
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
	message_all_servers(Message, Servers).
	
% Send a message to all servers EXCEPT ExcludeMe (so the server doesn't propose a value to itself)
message_all_servers(Message, ExcludeMe) when is_pid(ExcludeMe) ->
	Servers = lists:filter(fun(X) -> X /= ExcludeMe end, get_servers()),
	message_all_servers(Message, Servers);
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

collect_replies(0, _ThingToMatchAgainst, Replies) ->
    Replies;
collect_replies(N, ThingToMatchAgainst, Replies) ->
	% I don't want to write the same collecting code a bunch of times, so this should work for different situations, but
	% it might not be a great idea. Let's see what happens...
    receive
		% Client is waiting for responses to a read request
		{read_value, _Server, Value} ->	collect_replies(N-1, ThingToMatchAgainst, [Value|Replies]);
		% Server is waiting for responses to a promise request (two lines):
		{promise, ThingToMatchAgainst, accept, {HighestSeq, HighestSeqValue}} -> collect_replies(N-1, ThingToMatchAgainst, [{HighestSeq, HighestSeqValue} | Replies]);
		{promise, ThingToMatchAgainst, reject} -> collect_replies(N-1, ThingToMatchAgainst, Replies);
		% Server is waiting for responses to an accept request (two lines):
		{accept_ack, ThingToMatchAgainst, accept} ->
			collect_replies(N-1, ThingToMatchAgainst, [true| Replies]); %Doesn't really matter what we put in the reply list here...
		{accept_ack, ThingToMatchAgainst, reject} ->
			collect_replies(N-1, ThingToMatchAgainst, Replies)
    end.
% TODO: INTRODUCE A TIMEOUT SOMEWHERE IN HERE? ^^^

aggregate_list(List) ->
	aggregate_list(List, dict:new()).

aggregate_list([], Dict) ->
	dict:to_list(Dict);
aggregate_list([Key|T], Dict) ->
	aggregate_list(T, dict:update_counter(Key, 1, Dict)).

%Client logging
log_c(Message) -> log("CLIENT", Message, []).
log_c(Message, Args) -> log("CLIENT", Message, Args).

%Server logging
log(Message) -> log("SERVER", Message, []).
log(Message, Args) -> log("SERVER", Message, Args).

log(ProcessType, Message, Args) ->
	ArgsFormat = lists:flatten(["~p " || _ <- lists:seq(1, length(Args))]),
	io:format("~s ~p - ~s " ++ ArgsFormat ++ "~n", [ProcessType, self(), Message] ++ Args).

%---------------------------------------------------------------------------------------------
% Unit tests
%---------------------------------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

all_test() ->
	start(),
	?assertEqual({nothing, 3}, read()), %Initial value
	write(testing_testing),
	timer:sleep(50), % Wait for the paxos round to complete...
	?assertEqual({testing_testing, 3}, read()),
	stop().

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

highest_proposal_test() ->
	R1 = make_ref(),
	R2 = make_ref(),
	R3 = make_ref(),
	Proposals = [{R1, a}, {R2, b}, {R3, c}],
	?assertEqual({0, nothing}, highest_proposal([])),
	?assertEqual({R3, c}, highest_proposal(Proposals)).
