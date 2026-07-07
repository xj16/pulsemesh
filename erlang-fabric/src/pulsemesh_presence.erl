%%%-------------------------------------------------------------------
%%% @doc Per-channel presence tracker (one gen_server per active channel).
%%%
%%% Holds the live presence map for a single channel: user-id -> state
%%% ("online" | "away" | "busy" | "offline"). It is fed by the AMQP consumer
%%% as domain events arrive, and can be queried for a snapshot.
%%%
%%% Rehydration: on start it fires a `handle_continue` that replays the
%%% channel's event log from the command/query service and folds it into
%%% state, so a tracker that just crashed (or started after events were
%%% already published and acked) comes back with the correct presence instead
%%% of waiting for the next live event. Each applied event's version is
%%% tracked so a live event that overlaps the replay window is not applied
%%% twice. Replay is best-effort: if the log is unreachable the tracker keeps
%%% its (possibly empty) state and rehydrates from subsequent live events.
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

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         handle_continue/2, terminate/2]).

-record(state, {
    channel_id :: binary(),
    members    :: #{binary() => binary()},  %% user-id -> presence-state
    version    :: non_neg_integer()          %% highest event version applied
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
    %% Return fast, then rehydrate from the log in handle_continue so a slow or
    %% unreachable command/query service never blocks tracker startup.
    {ok, #state{channel_id = ChannelId, members = #{}, version = 0},
     {continue, rehydrate}}.

handle_continue(rehydrate, #state{channel_id = ChannelId} = State) ->
    %% Replay is opt-outable (env `replay_enabled`, default true) so tests and
    %% offline runs don't attempt an HTTP call to the command/query service.
    case application:get_env(pulsemesh_fabric, replay_enabled, true) of
        false ->
            {noreply, State};
        _ ->
            case pulsemesh_replay:events_since(ChannelId, 0) of
                {ok, Events} ->
                    NewState = lists:foldl(fun project/2, State, Events),
                    case NewState#state.version of
                        0 -> ok;
                        V -> logger:info("presence ~s rehydrated from log to v~p "
                                         "(~p members)",
                                         [ChannelId, V,
                                          maps:size(NewState#state.members)])
                    end,
                    {noreply, NewState};
                {error, Reason} ->
                    logger:debug("presence ~s replay skipped (~p); will "
                                 "rehydrate from live events",
                                 [ChannelId, Reason]),
                    {noreply, State}
            end
    end.

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
%%
%% `bump/2` records the highest version applied so a live event that overlaps
%% the replay window is idempotently ignored (events are versioned per stream).
%%====================================================================

project(#{<<"version">> := V}, #state{version = Cur} = S)
  when is_integer(V), V =< Cur ->
    %% Already applied (e.g. seen during replay); skip to stay idempotent.
    S;

project(#{<<"type">> := <<"member-joined">>, <<"user-id">> := U} = E, S) ->
    bump(E, S#state{members = maps:put(U, <<"online">>, S#state.members)});

project(#{<<"type">> := <<"member-left">>, <<"user-id">> := U} = E, S) ->
    bump(E, S#state{members = maps:remove(U, S#state.members)});

project(#{<<"type">> := <<"presence-changed">>,
          <<"user-id">> := U, <<"presence">> := P} = E, S) ->
    case maps:is_key(U, S#state.members) of
        true  -> bump(E, S#state{members = maps:put(U, P, S#state.members)});
        false -> bump(E, S)
    end;

project(E, S) ->
    bump(E, S).

bump(#{<<"version">> := V}, S) when is_integer(V), V > S#state.version ->
    S#state{version = V};
bump(_E, S) ->
    S.

%%====================================================================
%% Internal: via-tuple registration keyed by channel id.
%%====================================================================

via(ChannelId) ->
    {via, pulsemesh_via, {presence, ChannelId}}.
