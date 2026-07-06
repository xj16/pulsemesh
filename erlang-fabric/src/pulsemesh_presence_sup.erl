%%%-------------------------------------------------------------------
%%% @doc Dynamic supervisor for per-channel presence trackers.
%%%
%%% One `pulsemesh_presence` gen_server is started on demand per active
%%% channel (simple_one_for_one). Each tracker owns the authoritative live
%%% presence map for its channel inside the node. If a tracker crashes it is
%%% restarted with fresh state and rehydrated from the next events it sees —
%%% presence is a projection, so a lost tracker is a recoverable event, not a
%%% data-loss event.
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_presence_sup).
-behaviour(supervisor).

-export([start_link/0, ensure_tracker/1, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Return the pid of the presence tracker for `ChannelId`, starting one
%% if it does not yet exist. Safe under races: an {already_started, Pid}
%% result is treated as success.
-spec ensure_tracker(binary()) -> {ok, pid()}.
ensure_tracker(ChannelId) ->
    case supervisor:start_child(?MODULE, [ChannelId]) of
        {ok, Pid}                       -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, Reason}                 -> {error, Reason}
    end.

init([]) ->
    SupFlags = #{
        strategy  => simple_one_for_one,
        intensity => 20,
        period    => 10
    },
    ChildSpec = #{
        id       => pulsemesh_presence,
        start    => {pulsemesh_presence, start_link, []},
        restart  => transient,
        shutdown => 5000,
        type     => worker,
        modules  => [pulsemesh_presence]
    },
    {ok, {SupFlags, [ChildSpec]}}.
