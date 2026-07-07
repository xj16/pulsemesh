%%%-------------------------------------------------------------------
%%% @doc Log-replay client for the command/query service.
%%%
%%% The Postgres event log (owned by the Clojure `command-query` service) is
%%% the source of truth. When a presence tracker starts up empty — because it
%%% just crashed and was restarted, or the fabric reconnected after an outage
%%% — it rehydrates by asking the command/query service for the channel's
%%% events, folding them into presence state exactly the way the live consumer
%%% does. This closes the replay gap the README's failure model describes.
%%%
%%% Uses OTP's built-in `httpc` (inets); no extra deps. If the command/query
%%% service is unreachable the call returns `{error, Reason}` and the caller
%%% simply proceeds with empty state and rehydrates from subsequent live
%%% events, so replay is an optimization, never a hard dependency.
%%% @end
%%%-------------------------------------------------------------------
-module(pulsemesh_replay).

-export([events_since/2, base_url/0]).

%% @doc Default command/query base URL, overridable via env/OS.
-spec base_url() -> string().
base_url() ->
    application:get_env(pulsemesh_fabric, cq_url, "http://command-query:8080").

%% @doc Fetch the events for `ChannelId` with version > `Since` from the
%% command/query replay endpoint. Returns a list of event maps (binary keys,
%% same shape the AMQP consumer sees) or `{error, Reason}`.
-spec events_since(binary(), non_neg_integer()) ->
        {ok, [map()]} | {error, term()}.
events_since(ChannelId, Since) ->
    ensure_inets(),
    Url = lists:flatten(
            io_lib:format("~s/api/channels/~s/events?since=~p",
                          [base_url(), http_uri_encode(ChannelId), Since])),
    Request = {Url, []},
    HttpOpts = [{timeout, 3000}, {connect_timeout, 2000}],
    case httpc:request(get, Request, HttpOpts, [{body_format, binary}]) of
        {ok, {{_Vsn, 200, _Reason}, _Hdrs, Body}} ->
            decode_events(Body);
        {ok, {{_Vsn, Code, _Reason}, _Hdrs, _Body}} ->
            {error, {http_status, Code}};
        {error, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% Internal
%%====================================================================

ensure_inets() ->
    _ = application:ensure_all_started(inets),
    ok.

decode_events(Body) ->
    try jsx:decode(Body, [return_maps]) of
        #{<<"events">> := Events} when is_list(Events) -> {ok, Events};
        _ -> {ok, []}
    catch
        error:badarg -> {error, bad_json}
    end.

%% Minimal percent-encoding for the channel-id path segment (it is a
%% user-controlled string). Encodes everything outside the unreserved set.
http_uri_encode(Bin) when is_binary(Bin) ->
    http_uri_encode(binary_to_list(Bin));
http_uri_encode(Str) when is_list(Str) ->
    lists:flatten([enc(C) || C <- Str]).

enc(C) when C >= $A, C =< $Z -> C;
enc(C) when C >= $a, C =< $z -> C;
enc(C) when C >= $0, C =< $9 -> C;
enc(C) when C =:= $-; C =:= $_; C =:= $.; C =:= $~ -> C;
enc(C) -> io_lib:format("%~2.16.0B", [C]).
