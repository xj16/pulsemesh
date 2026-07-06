%%%-------------------------------------------------------------------
%%% @doc PulseMesh fabric OTP application callback.
%%%
%%% On start it:
%%%   1. brings up the supervision tree (pulsemesh_fabric_sup)
%%%   2. compiles the Cowboy routes and starts the HTTP/WebSocket listener
%%%
%%% Clients connect over WebSocket at /ws?channel=<id>&user=<id>. Committed
%%% domain events arriving from RabbitMQ are fanned out to every socket
%%% subscribed to the relevant channel.
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_fabric_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    %% Create the via-registry table up front so it survives independently of
    %% any single registered process (it is a public, named ETS table).
    ok = pulsemesh_via:ensure_table(),
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/ws", pulsemesh_ws_handler, []},
            {"/healthz", pulsemesh_health_handler, []}
        ]}
    ]),
    Port = application:get_env(pulsemesh_fabric, http_port, 8090),
    {ok, _} = cowboy:start_clear(
        pulsemesh_http_listener,
        [{port, Port}],
        #{env => #{dispatch => Dispatch}}
    ),
    logger:info("PulseMesh fabric HTTP/WS listening on port ~p", [Port]),
    pulsemesh_fabric_sup:start_link().

stop(_State) ->
    _ = cowboy:stop_listener(pulsemesh_http_listener),
    ok.
