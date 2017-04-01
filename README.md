# Erlang bindings for [ZeroMQ](http://www.zeromq.org)

## Building
Bindings work against [zeromq/ZeroMQ2-x](https://github.com/zeromq/zeromq2-x)'s `master` branch.
```bash
$ git clone git@github.com:zeromq/erlzmq.git
$ cd erlzmq
$ ./bootstrap
$ ./configure --with-zeromq=/path/to/zeromq
$ make
$ make docs
```

## Usage Samples

- Sample ZMQ_REQ client

```bash
$ erl +K true -smp disable -pa /path/to/zmq/ebin
```

```Erlang REPL
% Create ZeroMQ context
1> zmq:start_link().
{ok,<0.34.0>}

% Create a ZeroMQ REQ socket and specify that messages
% are delivered to the current shell's mailbox.
2> {ok, S} = zmq:socket(req, [{active, true}]).
{ok,{#Port<0.623>,1}}

% Connect to server
3> zmq:connect(S, "tcp://127.0.0.1:5555").
ok

% Send a message to server
4> zmq:send(S, <<"Test">>).
ok

% Receive a reply
5> zmq:recv(S).
{error,einval}

% Note the error - in the active socket mode we cannot
% receive messages by calling zmq:recv/1. Instead
% use receive keyword to accomplish the task.

6> f(M), receive M -> M end.
% The process blocks because there's no reply from server
% Once you start the server as shown in the following steps
% the receive call returns with the following message:
{zmq,1,<<"Reply">>}
```

- Sample ZMQ_REP server

Start another shell either within the same Erlang VM by using ^G, or in a separate OS shell:

```bash
$ erl +K true -smp disable -pa /path/to/zmq/ebin
```

```Erlang REPL
1> zmq:start_link().
{ok,<0.34.0>}
2> {ok, S} = zmq:socket(rep, [{active, false}]).
{ok,{#Port<0.483>,1}}
3> zmq:bind(S, "tcp://127.0.0.1:5555").
ok
4> zmq:recv(S).
{ok,<<"Test">>}
5> zmq:send(S, <<"Reply">>).
ok
```

You can run a server and any number of clients in the same Erlang shell or on different nodes.

See [https://zeromq.github.io/erlzmq/](https://zeromq.github.io/erlzmq/) for more examples and full documentation.
(Content is uploaded from doc/index.html).

## Updating documentation on GitHub
```bash
$ git checkout master   # make sure you are on master branch
$ make gitdocs          # this will update gh-pages branch and commit changes to origin
```

## License
BSD License

## Copyright
- Copyright (c) 2010 Dhammika Pathirana
- Copywight (c) 2010 Serge Aleynikov

## Contact
* Through Github [Issues](http://github.com/zeromq/erlzmq/issues)

## Copying
Erlang bindings are released under open-source BSD License (see LICENSE file)
