%%%-------------------------------------------------------------------
%%% @doc Top-level supervisor for the PulseMesh fabric.
%%%
%%% Supervision tree (rest_for_one: if the registry dies, the things that
%%% depend on it are restarted in order too):
%%%
%%%   pulsemesh_fabric_sup (rest_for_one)
%%%     |-- pulsemesh_channel_registry   (ETS-backed channel -> subscribers)
%%%     |-- pulsemesh_presence_sup       (simple_one_for_one: one child per
%%%     |                                 active channel presence tracker)
%%%     `-- pulsemesh_amqp_consumer      (RabbitMQ subscriber + fan-out)
%%%
%%% Supervised OTP processes survive crashes: a wedged presence tracker or a
%%% dropped AMQP connection is restarted automatically without taking the
%%% node down.
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_fabric_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy  => rest_for_one,
        intensity => 10,
        period    => 10
    },
    Children = [
        #{id       => pulsemesh_channel_registry,
          start    => {pulsemesh_channel_registry, start_link, []},
          restart  => permanent,
          shutdown => 5000,
          type     => worker,
          modules  => [pulsemesh_channel_registry]},

        #{id       => pulsemesh_presence_sup,
          start    => {pulsemesh_presence_sup, start_link, []},
          restart  => permanent,
          shutdown => 5000,
          type     => supervisor,
          modules  => [pulsemesh_presence_sup]},

        #{id       => pulsemesh_amqp_consumer,
          start    => {pulsemesh_amqp_consumer, start_link, []},
          restart  => permanent,
          shutdown => 5000,
          type     => worker,
          modules  => [pulsemesh_amqp_consumer]}
    ],
    {ok, {SupFlags, Children}}.
