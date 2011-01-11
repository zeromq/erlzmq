%% Basic PUB/SUB server for testing.
%% @hidden

-module(zmq_pubserver).
-export([run/0, run/2]).

run() -> run(10, 1000).
run(N, Delay) when is_integer(N), is_integer(Delay) ->
    spawn(fun() ->
        {ok ,Context} = zmq:init(1),
        case zmq:socket(Context, pub) of
        {ok, Socket} ->
            ok = zmq:bind(Socket, "tcp://127.0.0.1:5550"),
            send(Socket, Delay, N);
        Other ->
            io:format("~p error creating socket: ~p\n", [self(), Other])
        end,
        ok = zmq:term(Context)
    end).

send(Socket, _Delay, 0) ->
    io:format("~p pubserver is done.\n", [self()]),
    ok = zmq:close(Socket);
send(Socket, Delay, MsgIndex) ->
    Data = {msg, MsgIndex},
    case zmq:send(Socket, term_to_binary(Data)) of
    ok ->
        io:format("~p sent ~p\n", [self(), Data]),
        timer:sleep(Delay),
        send(Socket, Delay, MsgIndex-1);
    Other ->
        io:format("~p unexpected error in zmq:send(): ~p\n", [self(), Other])
    end.
