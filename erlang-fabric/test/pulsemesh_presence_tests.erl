%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the presence tracker projection.
%%%
%%% These drive the real gen_server through its public API (apply_event /
%%% snapshot) so they exercise the supervisor, the via-registry, and the
%%% event-folding logic together — the same path the AMQP consumer uses.
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_presence_tests).
-include_lib("eunit/include/eunit.hrl").

%% Fixture: bring up the via-registry table and the presence supervisor.
setup() ->
    ok = pulsemesh_via:ensure_table(),
    {ok, Sup} = pulsemesh_presence_sup:start_link(),
    Sup.

cleanup(Sup) ->
    unlink(Sup),
    exit(Sup, shutdown),
    ok.

presence_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun(_) ->
         [ join_marks_online()
         , presence_change_updates_state()
         , leave_removes_member()
         , presence_for_unknown_user_ignored()
         , unknown_channel_snapshot_is_empty()
         ]
     end}.

join_marks_online() ->
    Ch = <<"chan-join">>,
    ok = pulsemesh_presence:apply_event(Ch,
        #{<<"type">> => <<"member-joined">>,
          <<"channel-id">> => Ch, <<"user-id">> => <<"u1">>}),
    sync(Ch),
    ?_assertEqual(#{<<"u1">> => <<"online">>},
                  pulsemesh_presence:snapshot(Ch)).

presence_change_updates_state() ->
    Ch = <<"chan-pres">>,
    apply_all(Ch, [
        #{<<"type">> => <<"member-joined">>, <<"channel-id">> => Ch,
          <<"user-id">> => <<"u1">>},
        #{<<"type">> => <<"presence-changed">>, <<"channel-id">> => Ch,
          <<"user-id">> => <<"u1">>, <<"presence">> => <<"away">>}
    ]),
    sync(Ch),
    ?_assertEqual(#{<<"u1">> => <<"away">>},
                  pulsemesh_presence:snapshot(Ch)).

leave_removes_member() ->
    Ch = <<"chan-leave">>,
    apply_all(Ch, [
        #{<<"type">> => <<"member-joined">>, <<"channel-id">> => Ch,
          <<"user-id">> => <<"u1">>},
        #{<<"type">> => <<"member-joined">>, <<"channel-id">> => Ch,
          <<"user-id">> => <<"u2">>},
        #{<<"type">> => <<"member-left">>, <<"channel-id">> => Ch,
          <<"user-id">> => <<"u1">>}
    ]),
    sync(Ch),
    ?_assertEqual(#{<<"u2">> => <<"online">>},
                  pulsemesh_presence:snapshot(Ch)).

presence_for_unknown_user_ignored() ->
    Ch = <<"chan-ghost">>,
    apply_all(Ch, [
        #{<<"type">> => <<"presence-changed">>, <<"channel-id">> => Ch,
          <<"user-id">> => <<"ghost">>, <<"presence">> => <<"busy">>}
    ]),
    sync(Ch),
    ?_assertEqual(#{}, pulsemesh_presence:snapshot(Ch)).

unknown_channel_snapshot_is_empty() ->
    ?_assertEqual(#{}, pulsemesh_presence:snapshot(<<"never-seen">>)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

apply_all(Ch, Events) ->
    lists:foreach(fun(E) -> ok = pulsemesh_presence:apply_event(Ch, E) end,
                  Events).

%% Casts are async; a synchronous snapshot call flushes the mailbox so the
%% preceding casts are guaranteed processed before we assert.
sync(Ch) ->
    _ = pulsemesh_presence:snapshot(Ch),
    ok.
