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
-module(zmq_context).
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

-record(state, {owner, port}).

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
init(Config) when is_list(Config) ->
  process_flag(trap_exit, true),

  %NOTE: Sharing a single port amongst all sockets could be a bottleneck.  It
  %      may be interesting to look at using a port per socket rather than per
  %      context as we do now, of course updating the driver code as needed.

  case zmq_drv:load() of
    {ok, Port} ->
      IoThreads = proplists:get_value(iothreads, Config, 1),
      Owner = proplists:get_value(owner, Config),
      case zmq_drv:init(Port, IoThreads) of
        ok -> {ok, #state{owner = Owner, port = Port}};
        Error -> {stop, Error}
      end
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
handle_call(port, _From, State) ->
  {reply, {ok, State#state.port}, State}
;
handle_call(term, _From, State) ->
  case zmq_drv:term(State#state.port) of
    {error, eagain} -> {reply, {error, eagain}, State};
    _ -> {stop, normal, ok, State}
  end
;
handle_call(Request, From, State) ->
  {stop, {unhandled_call, Request, From}, State}
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
handle_info({'EXIT', Port, Reason}, #state{port=Port} = State) ->
  {stop, {port_died, Reason}, State#state{port = undefined}}
;
handle_info({'EXIT', Owner, Reason}, #state{owner=Owner} = State) ->
  {stop, {owner_died, Reason}, State#state{owner = undefined}}
;
handle_info(Info, State) ->
  {stop, {unhandled_info, Info}, State}
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @private
%% @doc Handle termination/shutdown.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
terminate(_Reason, #state{port=undefined}) ->
  ?log("terminate: ~p", [_Reason]),
  zmq_drv:unload(undefined)
;
terminate(normal, #state{port=Port}) ->
  ?log("terminate: ~p", [normal]),
  zmq_drv:unload(Port)
;
terminate(_Reason, #state{port=Port}) ->
  ?log("terminate: ~p", [_Reason]),
  %% Explicitly call zmq_term() to give a chance for sockets owners to clean up
  %% gracefully.  Else, forcefully close all sockets and terminate the context
  %% after a short period by closing the port and unloading the driver.
  case zmq_drv:term(Port) of
    {error, eagain} -> receive after 2000 -> ok end;
    _ -> ok
  end,
  zmq_drv:unload(Port)
.

%%==============================================================================
%% End Of File
