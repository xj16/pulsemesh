%%%-------------------------------------------------------------------
%%% @doc In-node pub/sub registry mapping channels to live subscriber pids.
%%%
%%% Every WebSocket handler process subscribes to the channel(s) it serves.
%%% When an event is fanned out we look up all subscriber pids for that
%%% channel and message them directly. Subscribers are monitored so their
%%% entries are cleaned up automatically when a connection dies.
%%%
%%% Backed by two ETS tables owned by this gen_server:
%%%   subs : bag of {ChannelId, Pid}
%%%   mons : {MonitorRef, ChannelId, Pid}  (for demonitor/cleanup on DOWN)
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_channel_registry).
-behaviour(gen_server).

-export([start_link/0,
         subscribe/1,
         unsubscribe/1,
         subscribers/1,
         broadcast/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SUBS, pulsemesh_subs).
-define(MONS, pulsemesh_mons).

%%====================================================================
%% Public API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Subscribe the calling process to a channel's fan-out stream.
-spec subscribe(binary()) -> ok.
subscribe(ChannelId) ->
    gen_server:call(?MODULE, {subscribe, ChannelId, self()}).

%% @doc Remove the calling process from a channel.
-spec unsubscribe(binary()) -> ok.
unsubscribe(ChannelId) ->
    gen_server:call(?MODULE, {unsubscribe, ChannelId, self()}).

%% @doc List subscriber pids for a channel.
-spec subscribers(binary()) -> [pid()].
subscribers(ChannelId) ->
    [Pid || {_C, Pid} <- ets:lookup(?SUBS, ChannelId)].

%% @doc Deliver `Msg` to every subscriber of `ChannelId`. Returns the count.
-spec broadcast(binary(), term()) -> non_neg_integer().
broadcast(ChannelId, Msg) ->
    Pids = subscribers(ChannelId),
    lists:foreach(fun(Pid) -> Pid ! {pulsemesh_event, Msg} end, Pids),
    length(Pids).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    ?SUBS = ets:new(?SUBS, [bag, named_table, protected, {read_concurrency, true}]),
    ?MONS = ets:new(?MONS, [set, named_table, protected]),
    {ok, #{}}.

handle_call({subscribe, ChannelId, Pid}, _From, State) ->
    case ets:match_object(?SUBS, {ChannelId, Pid}) of
        [] ->
            true = ets:insert(?SUBS, {ChannelId, Pid}),
            Ref = erlang:monitor(process, Pid),
            true = ets:insert(?MONS, {Ref, ChannelId, Pid}),
            ok;
        _Already ->
            ok
    end,
    {reply, ok, State};

handle_call({unsubscribe, ChannelId, Pid}, _From, State) ->
    do_unsubscribe(ChannelId, Pid),
    {reply, ok, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% A subscriber process died: purge all of its subscriptions.
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case ets:lookup(?MONS, Ref) of
        [{Ref, ChannelId, Pid}] ->
            ets:delete_object(?SUBS, {ChannelId, Pid}),
            ets:delete(?MONS, Ref);
        _ ->
            ok
    end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal
%%====================================================================

do_unsubscribe(ChannelId, Pid) ->
    ets:delete_object(?SUBS, {ChannelId, Pid}),
    %% Drop and demonitor any matching monitor entries.
    Matches = ets:match_object(?MONS, {'_', ChannelId, Pid}),
    lists:foreach(
        fun({Ref, _C, _P}) ->
            erlang:demonitor(Ref, [flush]),
            ets:delete(?MONS, Ref)
        end,
        Matches),
    ok.
