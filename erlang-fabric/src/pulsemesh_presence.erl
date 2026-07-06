%%%-------------------------------------------------------------------
%%% @doc Per-channel presence tracker (one gen_server per active channel).
%%%
%%% Holds the live presence map for a single channel: user-id -> state
%%% ("online" | "away" | "busy" | "offline"). It is fed by the AMQP consumer
%%% as domain events arrive, and can be queried for a snapshot.
%%%
%%% It registers itself under a via-tuple keyed on the channel id (using a
%%% local process registry table) so callers can address it by channel
%%% without threading pids around.
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_presence).
-behaviour(gen_server).

-export([start_link/1,
         apply_event/2,
         snapshot/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    channel_id :: binary(),
    members    :: #{binary() => binary()}  %% user-id -> presence-state
}).

%%====================================================================
%% Public API
%%====================================================================

start_link(ChannelId) ->
    gen_server:start_link(via(ChannelId), ?MODULE, [ChannelId], []).

%% @doc Feed a decoded domain event (a map with binary keys) into the tracker
%% for its channel, starting the tracker if needed.
-spec apply_event(binary(), map()) -> ok.
apply_event(ChannelId, Event) ->
    {ok, _Pid} = pulsemesh_presence_sup:ensure_tracker(ChannelId),
    gen_server:cast(via(ChannelId), {event, Event}).

%% @doc Return the current presence map for a channel (empty if no tracker).
-spec snapshot(binary()) -> #{binary() => binary()}.
snapshot(ChannelId) ->
    try
        gen_server:call(via(ChannelId), snapshot)
    catch
        exit:{noproc, _} -> #{}
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([ChannelId]) ->
    {ok, #state{channel_id = ChannelId, members = #{}}}.

handle_call(snapshot, _From, State) ->
    {reply, State#state.members, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({event, Event}, State) ->
    {noreply, project(Event, State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal: fold one event into presence state.
%%====================================================================

project(#{<<"type">> := <<"member-joined">>, <<"user-id">> := U}, S) ->
    S#state{members = maps:put(U, <<"online">>, S#state.members)};

project(#{<<"type">> := <<"member-left">>, <<"user-id">> := U}, S) ->
    S#state{members = maps:remove(U, S#state.members)};

project(#{<<"type">> := <<"presence-changed">>,
          <<"user-id">> := U, <<"presence">> := P}, S) ->
    case maps:is_key(U, S#state.members) of
        true  -> S#state{members = maps:put(U, P, S#state.members)};
        false -> S
    end;

project(_Other, S) ->
    S.

%%====================================================================
%% Internal: via-tuple registration keyed by channel id.
%%====================================================================

via(ChannelId) ->
    {via, pulsemesh_via, {presence, ChannelId}}.
