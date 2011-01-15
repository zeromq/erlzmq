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
%%------------------------------------------------------------------------------
%% @type zmq_context() = pid() | atom().
%% @type zmq_error()   = ebusy | enosys | edead | eterm | efault | einval
%%                     | eagain | enotsup | efsm | emthread | eprotonosupport
%%                     | enocompatproto | eaddrinuse | eaddrnotavail | enodev
%% @type zmq_event()   = pollin | pollout | pollerr.
%% @type zmq_socket()  = pid() | atom().
%% @type zmq_socket_type() =
%%                 pair |
%%                 pub  | sub | xpub | xsub |
%%                 req  | rep | xreq | xrep |
%%                 pull | push.
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
  socket/3,
  term/1
]).

-define(CONTEXT_TIMEOUT, infinity).
-define(SOCKET_TIMEOUT, infinity).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Bind a 0MQ socket to the given endpoint.
%% @spec (zmq_socket(), Endpoint) -> ok | {error, zmq_error()}
%%          Endpoint = string() | binary()
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
bind(Socket, Endpoint) when is_list(Endpoint) ->
  bind(Socket, list_to_binary(Endpoint))
;
bind(Socket, Endpoint) when is_binary(Endpoint) ->
  gen_server:call(Socket, {bind, [Endpoint]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Close a 0MQ socket.
%% @spec (zmq_socket()) -> ok | {error, zmq_error()}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
close(Socket) ->
  gen_server:call(Socket, close, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Connect a 0MQ socket to the given endpoint.
%% @spec (zmq_socket(), Endpoint) -> ok | {error, zmq_error()}
%%          Endpoint = string() | binary()
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
connect(Socket, Endpoint) when is_list(Endpoint) ->
  connect(Socket, list_to_binary(Endpoint))
;
connect(Socket, Endpoint) when is_binary(Endpoint) ->
  gen_server:call(Socket, {connect, [Endpoint]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Format returned 0MQ error atom into an error message string.
%% @spec (Error::zmq_error()) -> string()
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
%% @doc Get the value for the given 0MQ socket option.
%% @spec (zmq_socket(), atom()) -> {ok, Value} | {error, zmq_error()}
%%          Option = hwm | swap | affinity | identity | rate
%%                 | recovery_ivl | mcast_loop | sndbuf | rcvbuf | rcvmore
%%                 | linger | reconnect_ivl | backlog | fd | events | type
%%          Value = integer() | integer() | integer() | binary() | integer()
%%                | integer() | boolean() | integer() | integer() | boolean()
%%                | integer() | integer() | integer() | integer()
%%                | [zmq_event()] | zmq_socket_type()
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
getsockopt(Socket, Option) when is_atom(Option) ->
  gen_server:call(Socket, {getsockopt, [Option]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Initialize 0MQ context.
%% @spec (integer()) -> {ok, Context::pid()} | {error, zmq_error()}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
init(IoThreads) when is_integer(IoThreads) ->
  gen_server:start_link(zmq_context, [{owner, self()}, {iothreads, IoThreads}], [])
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Initialize 0MQ context. `Name' can be used as the `Context' in
%%      {@link {@module}:socket/2} and {@link {@module}:term/1}.  Typical use of
%%      this call is wrapping for placement in a supervision tree.
%% @spec (atom(), integer()) -> {ok, Context::pid()} | {error, zmq_error()}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
init(Name, IoThreads) when is_atom(Name), is_integer(IoThreads) ->
  gen_server:start_link({local, Name}, zmq_context, [{owner, self()}, {iothreads, IoThreads}], [])
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Poll for 0MQ socket events.  Events are received in the caller's mailbox
%%      as {@type {zmq, Socket::pid(), REvents::[zmq_event()]@}}.
%% @spec (zmq_socket(), [zmq_event()]) -> ok | {error, zmq_error()}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
poll(Socket, Events) when is_list(Events) ->
  gen_server:call(Socket, {poll, [Events]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Receive a message from the given 0MQ socket without blocking.
%% @spec (zmq_socket(), Flag::noblock) -> {ok, Data} | {error, zmq_error()}
%%          Data = binary() | [binary()]
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
recv(Socket, noblock) ->
  gen_server:call(Socket, {recv, [[noblock]]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Receive a message from the given 0MQ socket.
%% @spec (zmq_socket()) -> {ok, Data} | {error, zmq_error()}
%%          Data = binary() | [binary()]
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
recv(Socket) ->
  gen_server:call(Socket, {recv, [[]]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Send a message to the given 0MQ socket without blocking.
%% @spec (zmq_socket(), Data, Flag::noblock) -> ok | {error, zmq_error()}
%%          Data = binary() | [binary()]
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
send(Socket, Data, noblock) when is_binary(Data); is_list(Data), 0 < length(Data) ->
  gen_server:call(Socket, {send, [Data, [noblock]]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Send a message to the given 0MQ socket.
%% @spec (zmq_socket(), Data) -> ok | {error, zmq_error()}
%%          Data = binary() | [binary()]
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
send(Socket, Data) when is_binary(Data); is_list(Data), 0 < length(Data)  ->
  gen_server:call(Socket, {send, [Data, []]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Set 0MQ socket options.
%% @spec (zmq_socket(), [Option]) -> ok | {error, zmq_error()}
%%          Option = {hwm, integer()}
%%                 | {swap, integer()}
%%                 | {affinity, integer()}
%%                 | {identity, binary()}
%%                 | {subscribe, binary()}
%%                 | {unsubscibe, binary()}
%%                 | {rate, integer()}
%%                 | {recovery_ivl, integer()}
%%                 | {mcast_loop, boolean()}
%%                 | {sndbuf, integer()}
%%                 | {rcvbuf, integer()}
%%                 | {linger, integer()}
%%                 | {reconnect_ivl, integer()}
%%                 | {backlog, integer()}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
setsockopt(Socket, Options) when is_list(Options) ->
  gen_server:call(Socket, {setsockopt, [Options]}, ?SOCKET_TIMEOUT)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Create a 0MQ socket.
%% @spec (zmq_context(), zmq_socket_type()) -> {ok, Socket::pid()} | {error, zmq_error()}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
socket(Context, Type) when is_pid(Context) orelse is_atom(Context), is_atom(Type) ->
  gen_server:start_link(zmq_socket, [{owner, self()}, {context, Context}, {type, Type}], [])
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Create a 0MQ socket. `Name' can be used as the `Socket' in other
%%      `{@module}' module calls.  Typical use of this call is wrapping for
%%      placement in a supervision tree.
%% @spec (zmq_context(), atom(), zmq_socket_type()) -> {ok, Socket::pid()} | {error, zmq_error()}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
socket(Context, Name, Type) when is_pid(Context) orelse is_atom(Context), is_atom(Name), is_atom(Type) ->
  gen_server:start_link({local, Name}, zmq_socket, [{owner, self()}, {context, Context}, {type, Type}], [])
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
%% @doc Terminate 0MQ context
%% @spec (zmq_context()) -> ok | {error, eagain}
%% @end
%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
term(Context) ->
  gen_server:call(Context, term, ?CONTEXT_TIMEOUT)
.

%%==============================================================================
%% End Of File
