#! /usr/bin/env escript
%%! -smp enable -pa ebin

main([]) ->
    io:format("Usage: ~p ConnectToAddr MsgSize, MsgCount\n", [escript:script_name()]),
    erlang:halt(1);
main([ConnectTo,MessageSizeStr,MessageCountStr]) ->
    {MessageSize, _} = string:to_integer(MessageSizeStr),
    {MessageCount, _} = string:to_integer(MessageCountStr),
    zmq:start_link(),
    {ok, Socket} = zmq:socket(pub),
    zmq:connect(Socket, ConnectTo),
    Msg = list_to_binary(lists:duplicate(MessageSize, 0)),
    loop(MessageCount, Socket, Msg).

loop(0, _, _) ->
    ok;
loop(N, S, Msg) ->
    zmq:send(S, Msg),
    loop(N-1, S, Msg).    
