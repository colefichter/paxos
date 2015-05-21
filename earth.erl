-module(earth).
-behaviour(gen_fsm).

-compile([export_all]).

start_link() -> gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

%---------------------------------------------------------------------------------------------
% Client API to invoke events/state transitions.
%---------------------------------------------------------------------------------------------
sunrise() -> gen_fsm:send_event(?MODULE, sunrise).
sunset() -> gen_fsm:send_event(?MODULE, sunset).

%---------------------------------------------------------------------------------------------
% FSM implementation
%---------------------------------------------------------------------------------------------
init([]) -> {ok, day, []}.

day(sunset, LoopData) ->
    io:format("Goodnight!~n"),
    {next_state, night, LoopData}.

night(sunrise, LoopData) ->
    io:format("Good morning!~n"),
    {next_state, day, LoopData}.

%---------------------------------------------------------------------------------------------
% Unused callbacks required by gen_fsm
%---------------------------------------------------------------------------------------------
handle_sync_event(_Any, _From, StateName, StateData) -> {ok, StateName, StateData}.
handle_info(_Any, StateName, StateData) -> {ok, StateName, StateData}.
handle_event(_Any, StateName, StateData) -> {ok, StateName, StateData}.
code_change(_OldVsn, StateName, StateData, _Extra) -> {ok, StateName, StateData}.
terminate(_Any, StateName, StateData) -> {ok, StateName, StateData}.