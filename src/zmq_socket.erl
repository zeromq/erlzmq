%%==============================================================================
%% File: $Id$
%%
%% @private
%% @doc Erlang bindings for ZeroMQ.
%%
%% @author Dhammika Pathirana <dhammika at gmail dot com>
%% @author Serge Aleynikov <saleyn at gmail dot com>.
%% @author Chris Rempel <csrl at gmx dot com>.
%% @copyright 2010 Dhammika Pathirana and Serge Aleynikov, 2011 Chris Rempel
%% @version {@version}
%% @end
%%==============================================================================
-module(zmq_socket).
-behaviour(gen_server).

%% Behavior callbacks
-export([
  code_change/3,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).

-include("zmq.hrl").

-record(state, {owner, port, contextref}).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Behavior callbacks
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @private
%% @doc Handle code change.
%%
%% @spec code_change(OldVsn, State, Extra) ->
%%          {ok, NewState}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
code_change(_OldVsn, State, _Extra) ->
  {ok, State}
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @private
%% @doc Handle start.
%%
%% @spec init(Args) ->
%%          {ok, State} |
%%          {ok, State, Timeout} |
%%          {ok, State, hibernate} |
%%          {stop, Reason} |
%%          ignore
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
init(Config) ->
  process_flag(trap_exit, true),
  Owner = proplists:get_value(owner, Config),
  Type = proplists:get_value(type, Config),
  Context = proplists:get_value(context, Config),
  {ok, Port} = gen_server:call(Context, port, infinity),
  case zmq_drv:socket(Port, Type) of
    ok ->
      {ok, #state{
        owner = Owner,
        port = Port,
        contextref = erlang:monitor(process, Context)
      }}
    ;
    Error -> {stop, Error}
  end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @private
%% @doc Handle synchronous call.
%%
%% @spec handle_call(Request, From, State) ->
%%          {reply, Reply, NewState} |
%%          {reply, Reply, NewState, Timeout} |
%%          {reply, Reply, NewState, hibernate} |
%%          {noreply, NewState} |
%%          {noreply, NewState, Timeout} |
%%          {noreply, NewState, hibernate} |
%%          {stop, Reason, Reply, NewState} |
%%          {stop, Reason, NewState}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
handle_call(Request, From, State) ->
  ?log("handle_call for ~p: ~p", [From, Request]),
  case Request of
    close ->
      Result = zmq_drv:close(State#state.port),
      ?log("result for ~p: ~p", [From, Result]),
      gen_server:reply(From, Result),
      {stop, normal, State}
    ;
    {send, [Parts, Flags]} when is_list(Parts) ->
      Result = send_parts(State#state.port, Parts, Flags),
      ?log("result for ~p: ~p", [From, Result]),
      {reply, Result, State}
    ;
    {recv, [Flags]} ->
      Result = recv_parts(State#state.port, Flags),
      ?log("result for ~p: ~p", [From, Result]),
      {reply, Result, State}
    ;
    {Method, Args} when is_atom(Method), is_list(Args) ->
      %TODO: ??? catch apply/3 in case bad Args content is provided and
      %  zmq_drv.erl code crashes. For example, an invalid getsockopt option
      %  being provided. Or should we validate Args content?
      Result = apply(zmq_drv, Method, [State#state.port | Args]),
      ?log("result for ~p: ~p", [From, Result]),
      {reply, Result, State}
    ;
    _ ->
      {stop, {unhandled_call, Request, From}, State}
  end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @private
%% @doc Handle asynchronous call.
%%
%% @spec handle_cast(Msg, State) ->
%%          {noreply, NewState} |
%%          {noreply, NewState, Timeout} |
%%          {noreply, NewState, hibernate} |
%%          {stop, Reason, NewState}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
handle_cast(Msg, State) ->
  {stop, {unhandled_cast, Msg}, State}
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @private
%% @doc Handle message.
%%
%% @spec handle_info(Info, State) ->
%%          {noreply, NewState} |
%%          {noreply, NewState, Timeout} |
%%          {noreply, NewState, hibernate} |
%%          {stop, Reason, NewState}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
handle_info({?DRIVER_NAME, Events}, #state{owner=Owner} = State) when is_list(Events) ->
  Owner ! {zmq, self(), Events},
  {noreply, State}
;
handle_info({'EXIT', Owner, Reason}, #state{owner=Owner} = State) ->
  {stop, {owner_died, Reason}, State#state{owner = undefined}}
;
handle_info({'DOWN', Contextref, process, _, Reason}, #state{contextref=Contextref} = State) ->
  {stop, {context_died, Reason}, State#state{contextref = undefined}}
;
handle_info(_Info, State) ->
  ?log("unhandled info message ~p", [_Info]),
  {noreply, State}
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @private
%% @doc Handle termination/shutdown.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
terminate(_Reason, _State) ->
  ?log("terminate: ~p", [_Reason]),
  ok
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Internal functions
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
recv_parts(Port, Flags) ->
  do_recv_parts(Port, Flags, [])
.
do_recv_parts(Port, Flags, Parts) ->
  case zmq_drv:recv(Port, Flags) of
    {ok, Part} ->
      case zmq_drv:getsockopt(Port, rcvmore) of
        {ok, true} -> do_recv_parts(Port, Flags, [Part | Parts]);
        {ok, false} when [] =:= Parts -> {ok, Part};
        {ok, false} -> {ok, lists:reverse([Part | Parts])};
        Error -> Error
      end
    ;
    Error -> Error
  end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
send_parts(Port, [Part], Flags) ->
  zmq_drv:send(Port, Part, Flags)
;
send_parts(Port, [Part | Parts], Flags) ->
  case zmq_drv:send(Port, Part, [sndmore | Flags]) of
    ok -> send_parts(Port, Parts, Flags);
    Error -> Error
  end
.

%%==============================================================================
%% End Of File
