%%%-------------------------------------------------------------------
%%% @doc RabbitMQ consumer + event fan-out engine.
%%%
%%% Connects to RabbitMQ, declares/binds an exclusive queue to the shared
%%% `pulsemesh.events` topic exchange with routing key `channel.#`, and for
%%% every delivered event:
%%%
%%%   1. decodes the JSON body into a map
%%%   2. feeds it to the channel's presence tracker (starting one if needed)
%%%   3. broadcasts it to every WebSocket subscribed to that channel
%%%
%%% As a supervised gen_server, a dropped broker connection crashes this
%%% process and the supervisor restarts it, which re-establishes the
%%% subscription. That is the OTP "let it crash + supervise" recovery model
%%% doing the reconnection work for us.
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_amqp_consumer).
-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    connection :: pid() | undefined,
    channel    :: pid() | undefined,
    exchange   :: binary()
}).

%% How long to wait before a supervised restart re-attempts a failed connect.
-define(RECONNECT_DELAY, 3000).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    %% Defer the actual connect so a broker that is briefly unavailable at
    %% boot does not crash-loop the whole node before Rabbit is ready.
    self() ! connect,
    Exchange = application:get_env(pulsemesh_fabric, amqp_exchange,
                                   <<"pulsemesh.events">>),
    {ok, #state{exchange = Exchange}}.

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(connect, State) ->
    case do_connect(State) of
        {ok, NewState} ->
            logger:info("AMQP consumer subscribed to exchange ~s",
                        [State#state.exchange]),
            {noreply, NewState};
        {error, Reason} ->
            logger:warning("AMQP connect failed (~p); retrying in ~pms",
                           [Reason, ?RECONNECT_DELAY]),
            erlang:send_after(?RECONNECT_DELAY, self(), connect),
            {noreply, State}
    end;

%% Broker confirmed our subscription.
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

%% A delivered event.
handle_info({#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Body}},
            #state{channel = Ch} = State) ->
    handle_delivery(Body),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State};

%% Connection or channel died: crash so the supervisor restarts us clean.
handle_info({'DOWN', _Ref, process, _Pid, Reason}, State) ->
    {stop, {amqp_down, Reason}, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{connection = Conn, channel = Ch}) ->
    catch amqp_channel:close(Ch),
    catch amqp_connection:close(Conn),
    ok.

%%====================================================================
%% Internal
%%====================================================================

do_connect(State) ->
    Uri = application:get_env(pulsemesh_fabric, amqp_uri,
                              "amqp://guest:guest@localhost:5672"),
    case amqp_uri:parse(Uri) of
        {ok, Params} ->
            case amqp_connection:start(Params) of
                {ok, Conn} ->
                    erlang:monitor(process, Conn),
                    {ok, Ch} = amqp_connection:open_channel(Conn),
                    erlang:monitor(process, Ch),
                    setup_topology(Ch, State#state.exchange),
                    {ok, State#state{connection = Conn, channel = Ch}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, {bad_uri, Reason}}
    end.

setup_topology(Ch, Exchange) ->
    %% Idempotently ensure the exchange exists (the Clojure side also declares
    %% it; declaring the same durable exchange twice is safe).
    #'exchange.declare_ok'{} =
        amqp_channel:call(Ch, #'exchange.declare'{exchange = Exchange,
                                                  type = <<"topic">>,
                                                  durable = true}),
    %% Exclusive, auto-deleting queue: this node's private fan-out feed.
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Ch, #'queue.declare'{exclusive = true,
                                               auto_delete = true}),
    #'queue.bind_ok'{} =
        amqp_channel:call(Ch, #'queue.bind'{queue = Q,
                                            exchange = Exchange,
                                            routing_key = <<"channel.#">>}),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q}, self()),
    ok.

handle_delivery(Body) ->
    try jsx:decode(Body, [return_maps]) of
        #{<<"channel-id">> := ChannelId} = Event ->
            %% Update the supervised presence tracker for this channel.
            catch pulsemesh_presence:apply_event(ChannelId, Event),
            %% Fan out to any live WebSocket subscribers.
            _ = pulsemesh_channel_registry:broadcast(ChannelId, Event),
            ok;
        _Other ->
            logger:warning("dropping event without channel-id"),
            ok
    catch
        error:badarg ->
            logger:warning("dropping non-JSON AMQP payload"),
            ok
    end.
