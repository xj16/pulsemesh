%%%-------------------------------------------------------------------
%%% @doc Cowboy WebSocket handler for a single client connection.
%%%
%%% A client connects to:  /ws?channel=<channel-id>&user=<user-id>
%%%
%%% On connect the handler subscribes to the channel's fan-out stream via the
%%% channel registry and pushes an initial presence snapshot. Thereafter every
%%% domain event broadcast for that channel is delivered to the socket as a
%%% JSON text frame. The socket also accepts client-sent JSON control frames
%%% ({"op":"ping"}) for keep-alive.
%%%
%%% The handler process IS the subscriber the registry tracks, so when the
%%% connection closes the registry's monitor fires and cleans up automatically.
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_ws_handler).
-behaviour(cowboy_websocket).

-export([init/2,
         websocket_init/1,
         websocket_handle/2,
         websocket_info/2,
         terminate/3]).

-record(state, {
    channel_id :: binary(),
    user_id    :: binary()
}).

%% Drop idle connections after 60s of silence (client should ping).
-define(IDLE_TIMEOUT, 60000).

%%====================================================================
%% cowboy_websocket callbacks
%%====================================================================

init(Req, _Opts) ->
    QsVals = cowboy_req:parse_qs(Req),
    ChannelId = proplists:get_value(<<"channel">>, QsVals),
    UserId    = proplists:get_value(<<"user">>, QsVals, <<"anonymous">>),
    case ChannelId of
        undefined ->
            %% No channel -> reject with 400 before upgrading.
            Req2 = cowboy_req:reply(400, #{<<"content-type">> => <<"text/plain">>},
                                    <<"missing 'channel' query parameter">>, Req),
            {ok, Req2, undefined};
        _ ->
            State = #state{channel_id = ChannelId, user_id = UserId},
            {cowboy_websocket, Req, State, #{idle_timeout => ?IDLE_TIMEOUT}}
    end.

websocket_init(#state{channel_id = ChannelId} = State) ->
    ok = pulsemesh_channel_registry:subscribe(ChannelId),
    %% Ensure a presence tracker exists and send the current snapshot.
    {ok, _} = pulsemesh_presence_sup:ensure_tracker(ChannelId),
    Snapshot = pulsemesh_presence:snapshot(ChannelId),
    Welcome = #{<<"op">>        => <<"welcome">>,
                <<"channel-id">> => ChannelId,
                <<"presence">>   => Snapshot},
    {[{text, jsx:encode(Welcome)}], State};

websocket_init(State) ->
    {ok, State}.

%% Client control frames.
websocket_handle({text, Data}, State) ->
    case decode_op(Data) of
        <<"ping">> ->
            {[{text, jsx:encode(#{<<"op">> => <<"pong">>})}], State};
        _Other ->
            {ok, State}
    end;

websocket_handle(_Frame, State) ->
    {ok, State}.

%% A domain event fanned out from the registry.
websocket_info({pulsemesh_event, Event}, State) ->
    {[{text, jsx:encode(Event)}], State};

websocket_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _Req, #state{channel_id = ChannelId}) ->
    catch pulsemesh_channel_registry:unsubscribe(ChannelId),
    ok;
terminate(_Reason, _Req, _State) ->
    ok.

%%====================================================================
%% Internal
%%====================================================================

decode_op(Data) ->
    try
        case jsx:decode(Data, [return_maps]) of
            #{<<"op">> := Op} -> Op;
            _ -> undefined
        end
    catch
        _:_ -> undefined
    end.
