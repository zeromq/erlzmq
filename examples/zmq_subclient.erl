%% Basic PUB/SUB client for testing.
%% @hidden

-module(zmq_subclient).
-export([run/0]).

run() ->
    spawn(fun() ->
        {ok ,Context} = zmq:init(1),
        case zmq:socket(Context, sub) of
        {ok, Socket} ->
            ok = zmq:setsockopt(Socket ,[{subscribe, ""}]),
            ok = zmq:connect(Socket, "tcp://127.0.0.1:5550"),
            loop(Socket);
        Other ->
            io:format("~p error creating socket: ~p\n", [self(), Other])
        end,
        ok = zmq:term(Context)
    end).

loop(Socket) ->
    case recv(Socket) of
    {ok, {msg, 1}} ->
        io:format("~p subclient is done.\n", [self()]),
        ok = zmq:close(Socket);
    {ok, {msg, N}} ->
        io:format("~p -> subclient received ~p\n", [self(), N]),
        loop(Socket);
    Other ->
        io:format("~p unexpected receive error: ~p\n", [self(), Other])
    end.

recv(Socket) ->
    case zmq:recv(Socket) of
    {ok, Msg} ->
        {ok, binary_to_term(Msg)};
    Error ->
        Error
    end.
