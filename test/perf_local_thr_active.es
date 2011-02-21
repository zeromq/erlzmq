#! /usr/bin/env escript
%%! -smp enable -pa ebin

main([]) ->
    io:format("Usage: ~p BindToAddr MsgSize, MsgCount\n", [escript:script_name()]),
    erlang:halt(1);
main([BindTo,MessageSizeStr,MessageCountStr]) ->
    {MessageSize, _} = string:to_integer(MessageSizeStr),
    {MessageCount, _} = string:to_integer(MessageCountStr),
    zmq:start_link(),
    {ok, Socket} = zmq:socket(sub, [{subscribe, ""},{active, true}]),
    ok = zmq:bind(Socket, BindTo),
    
    Start = now(),
    loop(MessageCount, Socket),
    Elapsed = timer:now_diff(now(), Start),
  
    Throughput = MessageCount / Elapsed * 1000000,
    Megabits = Throughput * MessageSize * 8,

    io:format("message size: ~p [B]~n"
              "message count: ~p~n"
              "mean throughput: ~.1f [msg/s]~n"
              "mean throughput: ~.1f [Mb/s]~n",
              [MessageSize, MessageCount, Throughput, Megabits]).    

loop(0, _) ->
    ok;
loop(N, S) ->
    receive X -> X end,
    loop(N-1, S).    
