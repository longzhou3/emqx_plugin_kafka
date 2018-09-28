%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(emqx_plugin_kafka).

-include("emqx_plugin_kafka.hrl").

-include_lib("emqx/include/emqx.hrl").

-export([load/1, unload/0]).

%% Hooks functions
-export([on_client_connected/4, on_client_disconnected/3]).
-export([on_client_subscribe/3, on_client_unsubscribe/3]).
-export([on_session_created/3, on_session_resumed/3, on_session_terminated/3]).
-export([on_session_subscribed/4, on_session_unsubscribed/4]).
-export([on_message_publish/2, on_message_delivered/3, on_message_acked/3, on_message_dropped/3]).

%% Called when the plugin application start
load(Env) ->
    ekaf_init([Env]),
    emqx:hook('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
    emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqx:hook('client.subscribe', fun ?MODULE:on_client_subscribe/3, [Env]),
    emqx:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3, [Env]),
    emqx:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
    emqx:hook('session.resumed', fun ?MODULE:on_session_resumed/3, [Env]),
    emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    emqx:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
    emqx:hook('session.terminated', fun ?MODULE:on_session_terminated/3, [Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqx:hook('message.delivered', fun ?MODULE:on_message_delivered/3, [Env]),
    emqx:hook('message.acked', fun ?MODULE:on_message_acked/3, [Env]),
    emqx:hook('message.dropped', fun ?MODULE:on_message_dropped/3, [Env]),
    io:format("start kafka plugin Reload.~n", []).

on_client_connected(#{client_id := ClientId}, ConnAck, ConnAttrs, _Env) ->
    io:format("Client(~s) connected, connack: ~w, conn_attrs:~p~n", [ClientId, ConnAck, ConnAttrs]),
    ekaf_send(<<"connected">>, ClientId, {}, _Env),
    {ok, ClientId}.

on_client_disconnected(#{client_id := ClientId}, ReasonCode, _Env) ->
    io:format("Client(~s) disconnected, reason_code: ~w~n", [ClientId, ReasonCode]),
    ekaf_send(<<"disconnected">>, ClientId, {}, _Env),
    ok.

on_client_subscribe(#{client_id := ClientId}, RawTopicFilters, _Env) ->
    io:format("Client(~s) will subscribe: ~p~n", [ClientId, RawTopicFilters]),
    {ok, RawTopicFilters}.

on_client_unsubscribe(#{client_id := ClientId}, RawTopicFilters, _Env) ->
    io:format("Client(~s) unsubscribe ~p~n", [ClientId, RawTopicFilters]),
    {ok, RawTopicFilters}.

on_session_created(#{client_id := ClientId}, SessAttrs, _Env) ->
    io:format("Session(~s) created: ~p~n", [ClientId, SessAttrs]).

on_session_resumed(#{client_id := ClientId}, SessAttrs, _Env) ->
    io:format("Session(~s) resumed: ~p~n", [ClientId, SessAttrs]).

on_session_subscribed(#{client_id := ClientId}, Topic, SubOpts, _Env) ->
    io:format("Session(~s) subscribe ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]),
    ekaf_send(<<"subscribed">>, ClientId, {Topic, SubOpts}, _Env),
    {ok, {Topic, SubOpts}}.

on_session_unsubscribed(#{client_id := ClientId}, Topic, Opts, _Env) ->
    io:format("Session(~s) unsubscribe ~s with opts: ~p~n", [ClientId, Topic, Opts]),
    ekaf_send(<<"unsubscribed">>, ClientId, {Topic, Opts}, _Env),
    ok.

on_session_terminated(#{client_id := ClientId}, ReasonCode, _Env) ->
    io:format("Session(~s) terminated: ~p.", [ClientId, ReasonCode]),
    stop.

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    io:format("Publish ~s~n", [emqx_message:format(Message)]),
    ekaf_send(<<"publish">>, {}, Message, _Env),
    {ok, Message}.

on_message_delivered(#{client_id := ClientId}, Message, _Env) ->
    io:format("Delivered message to client(~s): ~s~n", [ClientId, emqx_message:format(Message)]),
    {ok, Message}.

on_message_acked(#{client_id := ClientId}, Message, _Env) ->
    io:format("Session(~s) acked message: ~s~n", [ClientId, emqx_message:format(Message)]),
    {ok, Message}.

on_message_dropped(_By, #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    ok;
on_message_dropped(#{node := Node}, Message, _Env) ->
    io:format("Message dropped by node ~s: ~s~n", [Node, emqx_message:format(Message)]);
on_message_dropped(#{client_id := ClientId}, Message, _Env) ->
    io:format("Message dropped by client ~s: ~s~n", [ClientId, emqx_message:format(Message)]).

%% Called when the plugin application stop
unload() ->
    io:format("start kafka Test Unload.~n", []),
    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqx:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/3),
    emqx:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/3),
    emqx:unhook('session.created', fun ?MODULE:on_session_created/3),
    emqx:unhook('session.resumed', fun ?MODULE:on_session_resumed/3),
    emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    emqx:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    emqx:unhook('session.terminated', fun ?MODULE:on_session_terminated/3),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqx:unhook('message.delivered', fun ?MODULE:on_message_delivered/3),
    emqx:unhook('message.acked', fun ?MODULE:on_message_acked/3),
    emqx:unhook('message.dropped', fun ?MODULE:on_message_dropped/3).
    
%% ==================== ekaf_init STA.===============================%%
ekaf_init(_Env) ->
    % clique 方式读取配置文件
    Env = application:get_env(?APP, kafka),
    {ok, Kafka} = Env,
    Host = proplists:get_value(host, Kafka),
    Port = proplists:get_value(port, Kafka),
    Broker = {Host, Port},
    Topic = proplists:get_value(topic, Kafka),
    io:format("ekaf_init ~s ~w ~s ~n", [Host, Port, Topic]),

    % init kafka
    application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
    application:set_env(ekaf, ekaf_bootstrap_broker, Broker),
    application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(Topic)),
    %application:set_env(ekaf, ekaf_bootstrap_broker, {"127.0.0.1", 9092}),
    %application:set_env(ekaf, ekaf_bootstrap_topics, <<"test">>),

    %io:format("Init ekaf with ~s:~b~n", [Host, Port]),
    %%ekaf:produce_async_batched(<<"test">>, list_to_binary(Json)),
    ok.
%% ==================== ekaf_init END.===============================%%


%% ==================== ekaf_send STA.===============================%%
ekaf_send(Type, ClientId, {}, _Env) ->
    Json = emqx_json:safe_encode([
        		      {type, Type},
        		      {client_id, ClientId},
        		      {message, {}},
        		      {cluster_node, node()},
        		      {ts, emqx_time:now_ms()}
        		     ]),
    ekaf_send_sync(Json);
ekaf_send(Type, ClientId, {Reason}, _Env) ->
    Json = emqx_json:safe_encode([
			      {type, Type},
			      {client_id, ClientId},
			      {cluster_node, node()},
			      {message, Reason},
			      {ts, emqx_time:now_ms()}
			     ]),
    ekaf_send_sync(Json);
ekaf_send(Type, ClientId, {Topic, Opts}, _Env) ->
    Json = emqx_json:safe_encode([
			      {type, Type},
			      {client_id, ClientId},
			      {cluster_node, node()},
			      {message, [
					 {topic, Topic},
					 {opts, Opts}
					]},
			      {ts, emqx_time:now_ms()}
			     ]),
    ekaf_send_sync(Json);
ekaf_send(Type, _, Message, _Env) ->
    Id = Message#message.id,
    From = Message#message.from, %需要登录和不需要登录这里的返回值是不一样的
    Topic = Message#message.topic,
    Payload = Message#message.payload,
    Qos = Message#message.qos,
    Timestamp = Message#message.timestamp,

    ClientId = c(From),
    Username = u(From),
    Obj = [
        %%{id, Id},
        {type, Type},
        {client_id, ClientId},
        {message,
            [
                {username, Username},
                {topic, Topic},
                {payload, Payload},
                {qos, Qos},
                {timestamp, Timestamp}
            ]
        },
        {cluster_node, node()},
        {ts, emqx_time:now_ms()}
    ],
    io:format("publish ~w~n",Obj),
    Json = mochijson2:encode(Obj),
    ekaf_send_sync(Json).

ekaf_send_async(Msg) ->
    Topic = ekaf_get_topic(),
    ekaf_send_async(Topic, Msg).
ekaf_send_async(Topic, Msg) ->
    ekaf:produce_async_batched(list_to_binary(Topic), list_to_binary(Msg)).
ekaf_send_sync(Msg) ->
    Topic = ekaf_get_topic(),
    ekaf_send_sync(Topic, Msg).
ekaf_send_sync(Topic, Msg) ->
    ekaf:produce_sync_batched(list_to_binary(Topic), list_to_binary(Msg)).

i(true) -> 1;
i(false) -> 0;
i(I) when is_integer(I) -> I.
c({ClientId, Username}) -> ClientId;
c(From) -> From.
u({ClientId, Username}) -> Username;
u(From) -> From.
%% ==================== ekaf_send END.===============================%%


%% ==================== ekaf_set_host STA.===============================%%
ekaf_set_host(Host) ->
    ekaf_set_host(Host, 9092).
ekaf_set_host(Host, Port) ->
    Broker = {Host, Port},
    application:set_env(ekaf, ekaf_bootstrap_broker, Broker),
    io:format("reset ekaf Broker ~s:~b ~n", [Host, Port]),
    ok.
%% ==================== ekaf_set_host END.===============================%%

%% ==================== ekaf_set_topic STA.===============================%%
ekaf_set_topic(Topic) ->
    application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(Topic)),
    ok.
ekaf_get_topic() ->
    Env = application:get_env(?APP, kafka),
    {ok, Kafka} = Env,
    Topic = proplists:get_value(topic, Kafka),
    Topic.
%% ==================== ekaf_set_topic END.===============================%%
