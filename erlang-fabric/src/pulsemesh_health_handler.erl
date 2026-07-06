%%%-------------------------------------------------------------------
%%% @doc Trivial Cowboy handler for liveness checks (GET /healthz).
%%% Used by docker-compose healthchecks and CI smoke tests.
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_health_handler).
-behaviour(cowboy_handler).

-export([init/2]).

init(Req0, State) ->
    Body = jsx:encode(#{<<"status">> => <<"ok">>,
                        <<"service">> => <<"pulsemesh-fabric">>}),
    Req = cowboy_req:reply(200,
        #{<<"content-type">> => <<"application/json">>},
        Body, Req0),
    {ok, Req, State}.
