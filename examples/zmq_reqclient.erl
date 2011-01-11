%% Basic REQ/REP client for testing.
%% @hidden

-module(zmq_reqclient).
-export([run/0, run/2]).

run() -> run(10, 1000).
run(N, Delay) when is_integer(N), is_integer(Delay) ->
    spawn(fun() ->
        {ok ,Context} = zmq:init(1),
        case zmq:socket(Context, req) of
        {ok, Socket} ->
            ok = zmq:connect(Socket, "tcp://127.0.0.1:5550"),
            reqrep(Socket, Delay, N);
        Other ->
            io:format("~p error creating socket: ~p\n", [self(), Other])
        end,
        ok = zmq:term(Context)
    end).

reqrep(Socket, _Delay, 0) ->
    ok = zmq:send(Socket, term_to_binary(stop)),
    {ok, stopped} = recv(Socket),
    io:format("~p reqclient is done.\n", [self()]),
    ok = zmq:close(Socket);
reqrep(Socket, Delay, MsgIndex) ->
    Data = {msg, MsgIndex},
    case zmq:send(Socket, term_to_binary(Data)) of
    ok ->
        io:format("~p ~w sent ~p\n", [self(), ?MODULE, Data]),
        recv(Socket, MsgIndex),
        timer:sleep(Delay),
        reqrep(Socket, Delay, MsgIndex-1);
    Error ->
        io:format("~p unexpected error in zmq:send(): ~p\n", [self(), Error])
    end.

recv(Socket) ->
    case zmq:recv(Socket) of
    {ok, Msg} ->
        {ok, binary_to_term(Msg)};
    Error ->
        Error
    end.

recv(Socket, N) ->
    case recv(Socket) of
    {ok, {msg, N}} ->
        io:format("~p -> reqclient received ~p\n", [self(), N]);
    Other ->
        io:format("~p unexpected receive error: ~p\n", [self(), Other]),
        throw(Other)
    end.
