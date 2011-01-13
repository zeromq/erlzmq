%%==============================================================================
%% File: $Id$
%%
%% @doc Erlang bindings for ZeroMQ.
%%
%% @author Dhammika Pathirana <dhammika at gmail dot com>
%% @author Serge Aleynikov <saleyn at gmail dot com>.
%% @author Chris Rempel <csrl at gmx dot com>.
%% @copyright 2010 Dhammika Pathirana and Serge Aleynikov, 2011 Chris Rempel
%% @version {@version}
%% @end
%%==============================================================================
-module(zmq).

%% Public API
-export([
  bind/2,
  close/1,
  connect/2,
  format_error/1,
  getsockopt/2,
  init/1,
  init/2,
  poll/2,
  recv/1,
  recv/2,
  send/2,
  send/3,
  setsockopt/2,
  socket/2,
  term/1
]).

-define(CONTEXT_TIMEOUT, infinity).
-define(SOCKET_TIMEOUT, infinity).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Bind a 0MQ socket to address.
%% @spec (Socket, Address) -> ok | {error, Reason}
%%          Address = string() | binary()
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
bind(Socket, Address) when is_list(Address) ->
  bind(Socket, list_to_binary(Address))
;
bind(Socket, Address) when is_binary(Address) ->
  gen_server:call(Socket, {bind, [Address]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Close a 0MQ socket.
%% @spec (Socket) -> ok | {error, Reason}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
close(Socket) ->
  gen_server:call(Socket, close, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Connect a 0MQ socket to address.
%% @spec (Socket, Address) -> ok | {error, Reason}
%%          Address = string() | binary()
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
connect(Socket, Address) when is_list(Address) ->
  connect(Socket, list_to_binary(Address))
;
connect(Socket, Address) when is_binary(Address) ->
  gen_server:call(Socket, {connect, [Address]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Format returned error atom.
%% @spec (atom()) -> string()
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% Erlang Binding Specific
format_error(ebusy)             -> "The socket is busy";
format_error(enosys)            -> "Function not implemented";
format_error(edead)             -> "Socket is dead";
%% Common
format_error(eterm)             -> "The context has terminated";
format_error(efault)            -> "The socket is invalid";
format_error(einval)            -> "The parameters are invalid";
%% Send/Recv
format_error(eagain)            -> "Message can not sent or no message available at this time";
format_error(enotsup)           -> "Operation not supported by this socket type";
format_error(efsm)              -> "Operation cannot be accomplished in current state";
%% Socket
format_error(emthread)          -> "The maximum number of sockets has been exceeded";
%% Bind/Connect
format_error(eprotonosupport)   -> "The requested transport protocol is not supported";
format_error(enocompatproto)    -> "The requested transport protocol is not compatible with the socket type";
%% Bind
format_error(eaddrinuse)        -> "The requested address is already in use";
format_error(eaddrnotavail)     -> "The requested address is not local";
format_error(enodev)            -> "The requested address specifies a nonexistent interface".

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Get socket option.
%% @spec (Socket, Option) -> {ok, Value} | {error, Reason}
%%          Option = zmq_sockopt()
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
getsockopt(Socket, Option) when is_atom(Option) ->
  gen_server:call(Socket, {getsockopt, [Option]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Initialize owned 0MQ Context.
%% @spec (IoThreads) -> {ok, Context} | {error, Error}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
init(IoThreads) when is_integer(IoThreads) ->
  gen_server:start_link(zmq_context, [{owner, self()}, {iothreads, IoThreads}], [])
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Initialize unowned named 0MQ Context. Name can be used as the Context in
%%      zmq:socket/2 and zmq:term/1.
%% @spec (Name, IoThreads) -> {ok, Context} | {error, Error}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
init(Name, IoThreads) when is_atom(Name), is_integer(IoThreads) ->
  gen_server:start({local, Name}, zmq_context, [{iothreads, IoThreads}], [])
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Poll for socket events.  Events are received in the caller's mailbox
%%      as {zmq, Socket, REvents}. Where REvents is a list containing 'pollin'
%%      and/or 'pollout' or 'pollerr'.
%% @spec (Socket, Events) -> ok | {error, Reason}
%%          Events = [Event]
%%          Event = pollin | pollout | pollerr
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
poll(Socket, Events) when is_list(Events) ->
  gen_server:call(Socket, {poll, [Events]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Receive a message from a given 0MQ socket without blocking.
%% @spec (Socket, noblock) -> {ok, Data} | {error, Reason}
%%         Data = binary() | [binary()]
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
recv(Socket, noblock) ->
  gen_server:call(Socket, {recv, [[noblock]]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Receive a message from a given 0MQ socket.
%% @spec (Socket) -> {ok, Data} | {error, Reason}
%%         Data = binary() | [binary()]
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
recv(Socket) ->
  gen_server:call(Socket, {recv, [[]]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Send a message to a given 0MQ socket without blocking.
%% @spec (Socket, Data, noblock) -> ok | {error, Reason}
%%          Data = binary() | [binary()]
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
send(Socket, Data, noblock) when is_binary(Data); is_list(Data) andalso 0 < length(Data) ->
  gen_server:call(Socket, {send, [Data, [noblock]]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Send a message to a given 0MQ socket.
%% @spec (Socket, Data) -> ok | {error, Reason}
%%          Data = binary() | [binary()]
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
send(Socket, Data) when is_binary(Data); is_list(Data) andalso 0 < length(Data)  ->
  gen_server:call(Socket, {send, [Data, []]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Set socket options.
%% @spec (Socket, Options) -> ok | {error, Reason}
%%          Options = [{zmq_sockopt(), Value}]
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
setsockopt(Socket, Options) when is_list(Options) ->
  gen_server:call(Socket, {setsockopt, [Options]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Create a 0MQ socket.
%% @spec (Context, Type) -> {ok, Socket} | {error, Reason}
%%          Type = pair |
%%                 pub  | sub | xpub | xsub |
%%                 req  | rep | xreq | xrep |
%%                 pull | push
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
socket(Context, Type) when is_atom(Type); is_pid(Context) orelse is_atom(Context) ->
  gen_server:start_link(zmq_socket, [{owner, self()}, {context, Context}, {type, Type}], [])
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Terminate 0MQ Context
%% @spec (Context) -> ok | {error, eagain}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
term(Context) ->
  gen_server:call(Context, term, ?CONTEXT_TIMEOUT)
.

%%==============================================================================
%% End Of File
