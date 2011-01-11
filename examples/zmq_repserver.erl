%% Basic REQ/REP server for testing.
%% @hidden

-module(zmq_repserver).
-export([run/0]).

run() ->
    spawn(fun() ->
        {ok ,Context} = zmq:init(1),
        case zmq:socket(Context, rep) of
        {ok, Socket} ->
            ok = zmq:bind(Socket, "tcp://127.0.0.1:5550"),
            ok = zmq:poll(Socket, [pollin]),
            reqrep(Socket);
        Other ->
            io:format("~p error creating socket: ~p\n", [self(), Other])
        end,
        ok = zmq:term(Context)
    end).

reqrep(Socket) ->
    receive
    {zmq, Socket, [pollin]} ->
        case recv(Socket) of
        {ok, stop} ->
            ok = zmq:send(Socket, term_to_binary(stopped)),
            io:format("~p repserver is done.\n", [self()]),
            ok = zmq:close(Socket);
        {ok, {msg, N} = Msg} ->
            io:format("~p -> received ~p\n", [self(), N]),
            ok = zmq:send(Socket, term_to_binary(Msg)),
            io:format("~p <- replied  ~p\n", [self(), N]),
            ok = zmq:poll(Socket, [pollin]),
            reqrep(Socket);
        Other ->
            io:format("~p unexpected error in zmq:recv(): ~p\n", [self(), Other])
        end
    end.

recv(Socket) ->
    case zmq:recv(Socket, noblock) of
    {ok, Msg} ->
        {ok, binary_to_term(Msg)};
    Error ->
        Error
    end.
