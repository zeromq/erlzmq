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
-module(zmq_drv).

%% Public API
-export([
  load/0,
  unload/1,
  init/2,
  term/1,
  socket/2,
  close/1,
  setsockopt/2,
  getsockopt/2,
  bind/2,
  connect/2,
  send/3,
  recv/2,
  poll/2
]).

-include("zmq.hrl").

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
load() ->
  %% Build SearchDir for erl_ddll:load/2
  DirName = re:replace(
    filename:dirname(code:which(?MODULE)),
    "/?[^/]+/\\.\\.",
    "",
    [{return,list}]
  ),
  SearchDir = filename:join(filename:dirname(DirName), "priv"),
  ?log("init, lib path: ~s", [SearchDir]),

  %% Load the port driver
  case erl_ddll:load(SearchDir, ?DRIVER_NAME) of
    ok ->
      try open_port({spawn_driver, ?DRIVER_NAME}, [binary]) of
        Port -> {ok, Port}
      catch
        error:Reason -> {error, {port_error, Reason}}
      end
    ;
    Error -> Error
  end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
unload(Port) when is_port(Port) ->
  port_close(Port),
  erl_ddll:unload(?DRIVER_NAME)
;
unload(_Port) ->
  erl_ddll:unload(?DRIVER_NAME)
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
init(Port, IoThreads) ->
  port_command(Port, encode_init(IoThreads)),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
term(Port) ->
  port_command(Port, encode_term()),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
socket(Port, Type) ->
  port_command(Port, encode_socket(Type)),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
close(Port) ->
  port_command(Port, encode_close()),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
setsockopt(Port, Options) ->
  port_command(Port, encode_setsockopt(Options)),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
getsockopt(Port, Option) ->
  port_command(Port, encode_getsockopt(Option)),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
bind(Port, Address) ->
  port_command(Port, encode_bind(Address)),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
connect(Port, Address) ->
  port_command(Port, encode_connect(Address)),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
send(Port, Data, Flags) ->
  port_command(Port, encode_send(Data, Flags)),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
recv(Port, Flags) ->
  port_command(Port, encode_recv(Flags)),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
poll(Port, Events) ->
  port_command(Port, encode_poll(Events)),
  receive {?DRIVER_NAME, Result} -> Result end
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Internal functions
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_bind(Address) when is_binary(Address) ->
  %% Must zero terminate the address.
  <<(?ZMQ_BIND):8, Address/binary, 0>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_close() ->
  <<(?ZMQ_CLOSE):8>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_connect(Address) when is_binary(Address) ->
  %% Must zero terminate the address.
  <<(?ZMQ_CONNECT):8, Address/binary, 0>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_getsockopt(Option) ->
  OptId = case Option of
    hwm           -> ?ZMQ_HWM;
    swap          -> ?ZMQ_SWAP;
    affinity      -> ?ZMQ_AFFINITY;
    identity      -> ?ZMQ_IDENTITY;
    rate          -> ?ZMQ_RATE;
    recovery_ivl  -> ?ZMQ_RECOVERY_IVL;
    mcast_loop    -> ?ZMQ_MCAST_LOOP;
    sndbuf        -> ?ZMQ_SNDBUF;
    rcvbuf        -> ?ZMQ_RCVBUF;
    rcvmore       -> ?ZMQ_RCVMORE;
    linger        -> ?ZMQ_LINGER;
    reconnect_ivl -> ?ZMQ_RECONNECT_IVL;
    backlog       -> ?ZMQ_BACKLOG;
    fd            -> ?ZMQ_FD;
    events        -> ?ZMQ_EVENTS;
    type          -> ?ZMQ_TYPE
  end,
  <<(?ZMQ_GETSOCKOPT):8, OptId:8>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_init(IoThreads) when is_integer(IoThreads), IoThreads =< 255 ->
  <<(?ZMQ_INIT):8, IoThreads:8>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_poll(Events) when is_list(Events) ->
  <<(?ZMQ_POLL):8, (events_to_int(Events)):8>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_recv(Flags) when is_list(Flags) ->
  <<(?ZMQ_RECV):8, (flags_to_int(Flags)):8>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_send(Data, Flags) when is_binary(Data), is_list(Flags) ->
  <<(?ZMQ_SEND):8, (flags_to_int(Flags)):8, Data/binary>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_setsockopt(Options) when is_list(Options), length(Options) =< 255 ->
  Opts = [make_sockopt({O, V}) || {O, V} <- proplists:unfold(Options)],
  <<(?ZMQ_SETSOCKOPT):8, (length(Opts)):8, (list_to_binary(Opts))/binary>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_socket(Type) ->
  TypeID = case Type of
    pair   -> ?ZMQ_PAIR;
    pub    -> ?ZMQ_PUB;
    sub    -> ?ZMQ_SUB;
    req    -> ?ZMQ_REQ;
    rep    -> ?ZMQ_REP;
    xreq   -> ?ZMQ_XREQ;
    xrep   -> ?ZMQ_XREP;
    pull   -> ?ZMQ_PULL;
    push   -> ?ZMQ_PUSH;
    xpub   -> ?ZMQ_XPUB;
    xsub   -> ?ZMQ_XSUB
  end,
  <<(?ZMQ_SOCKET):8, TypeID:8>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
encode_term() ->
  <<(?ZMQ_TERM):8>>
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
events_to_int([]) -> 0;
events_to_int([H|T]) ->
  events_to_int(T) bor
    case H of
      pollin  -> ?ZMQ_POLLIN;
      pollout -> ?ZMQ_POLLOUT
    end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
flags_to_int([]) -> 0;
flags_to_int([H|T]) ->
  flags_to_int(T) bor
    case H of
      noblock -> ?ZMQ_NOBLOCK;
      sndmore -> ?ZMQ_SNDMORE
    end
.

%%-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
make_sockopt({hwm,           V}) when is_integer(V) -> <<?ZMQ_HWM,          8, V:64/native>>;
make_sockopt({swap,          V}) when is_integer(V) -> <<?ZMQ_SWAP,         8, V:64/native>>;
make_sockopt({affinity,      V}) when is_integer(V) -> <<?ZMQ_AFFINITY,     8, V:64/native>>;
make_sockopt({identity,      V}) when is_binary(V),
                                byte_size(V) =< 255 -> <<?ZMQ_IDENTITY,     (byte_size(V)):8, V/binary>>;
make_sockopt({identity,      V}) when is_list(V) -> make_sockopt({identity, list_to_binary(V)});
% Note that 0MQ doesn't limit the size of subscribe/unsubscribe options,
% but we do for simplicity. Larger size can be supported if option value
% byte length is increased from a single byte encoding to a multibyte encoding.
make_sockopt({subscribe,     V}) when is_binary(V),
                                byte_size(V) =< 255 -> <<?ZMQ_SUBSCRIBE,    (byte_size(V)):8, V/binary>>;
make_sockopt({subscribe,     V}) when is_list(V) -> make_sockopt({subscribe, list_to_binary(V)});
make_sockopt({unsubscribe,   V}) when is_binary(V),
                                byte_size(V) =< 255 -> <<?ZMQ_UNSUBSCRIBE,  (byte_size(V)):8, V/binary>>;
make_sockopt({unsubscribe,   V}) when is_list(V) -> make_sockopt({unsubscribe, list_to_binary(V)});
make_sockopt({rate,          V}) when is_integer(V) -> <<?ZMQ_RATE,         8, V:64/native>>;
make_sockopt({recovery_ivl,  V}) when is_integer(V) -> <<?ZMQ_RECOVERY_IVL, 8, V:64/native>>;
make_sockopt({mcast_loop,false})                    -> <<?ZMQ_MCAST_LOOP,   8, 0:64/native>>;
make_sockopt({mcast_loop, true})                    -> <<?ZMQ_MCAST_LOOP,   8, 1:64/native>>;
make_sockopt({sndbuf,        V}) when is_integer(V) -> <<?ZMQ_SNDBUF,       8, V:64/native>>;
make_sockopt({rcvbuf,        V}) when is_integer(V) -> <<?ZMQ_RCVBUF,       8, V:64/native>>;
make_sockopt({linger,        V}) when is_integer(V) -> <<?ZMQ_LINGER,       4, V:32/native>>;
make_sockopt({reconnect_ivl, V}) when is_integer(V) -> <<?ZMQ_RECONNECT_IVL,4, V:32/native>>;
make_sockopt({backlog,       V}) when is_integer(V) -> <<?ZMQ_BACKLOG,      4, V:32/native>>.
