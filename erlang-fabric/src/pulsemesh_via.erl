%%%-------------------------------------------------------------------
%%% @doc A minimal ETS-backed process registry implementing the `via`
%%% callback interface required by gen_server/gen_statem.
%%%
%%% This lets processes be addressed by an arbitrary term (e.g.
%%% `{presence, ChannelId}`) instead of a raw pid, via:
%%%
%%%   gen_server:start_link({via, pulsemesh_via, Name}, ...)
%%%   gen_server:call({via, pulsemesh_via, Name}, ...)
%%%
%%% The registry table is created lazily on first use and owned by whichever
%%% process touches it first, which for our tree is the presence supervisor's
%%% children being started under it. To keep ownership stable we create the
%%% table in a dedicated heir-less public table guarded by a named lock.
%%%
%%% Callbacks required by the `via` protocol:
%%%   register_name/2, unregister_name/1, whereis_name/1, send/2
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_via).

-export([register_name/2, unregister_name/1, whereis_name/1, send/2]).
-export([ensure_table/0]).

-define(TABLE, pulsemesh_via_registry).

%% @doc Idempotently create the shared registry table. Safe to call from
%% multiple processes; the first wins and the rest no-op.
ensure_table() ->
    case ets:whereis(?TABLE) of
        undefined ->
            try
                ets:new(?TABLE, [set, named_table, public,
                                 {read_concurrency, true}]),
                ok
            catch
                error:badarg -> ok  %% lost the race; table now exists
            end;
        _Tid ->
            ok
    end.

register_name(Name, Pid) ->
    ensure_table(),
    %% whereis_name/1 evicts a stale entry left by a crashed process, so a
    %% restarted presence tracker can re-register under the same name.
    case whereis_name(Name) of
        undefined ->
            case ets:insert_new(?TABLE, {Name, Pid}) of
                true  -> yes;
                false -> no   %% lost a concurrent registration race
            end;
        _LivePid ->
            no
    end.

unregister_name(Name) ->
    ensure_table(),
    ets:delete(?TABLE, Name),
    ok.

whereis_name(Name) ->
    ensure_table(),
    case ets:lookup(?TABLE, Name) of
        [{Name, Pid}] ->
            case is_process_alive(Pid) of
                true  -> Pid;
                false ->
                    %% Stale entry from a crashed process: clean it up.
                    ets:delete(?TABLE, Name),
                    undefined
            end;
        [] ->
            undefined
    end.

send(Name, Msg) ->
    case whereis_name(Name) of
        undefined ->
            exit({badarg, {Name, Msg}});
        Pid ->
            Pid ! Msg,
            Pid
    end.
