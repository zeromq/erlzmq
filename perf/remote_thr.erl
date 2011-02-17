#! /usr/bin/env escript
%%! -smp enable -pa ebin

main([ConnectTo,MessageSizeStr,MessageCountStr]) ->
    {MessageSize, _} = string:to_integer(MessageSizeStr),
    {MessageCount, _} = string:to_integer(MessageCountStr),
    zmq:start_link(),
    {ok, Socket} = zmq:socket(pub),
    zmq:connect(Socket, ConnectTo),
    Msg = list_to_binary(lists:duplicate(MessageSize, 0)),
    [ zmq:send(Socket, Msg) || _I <- lists:seq(1, MessageCount) ].
    
