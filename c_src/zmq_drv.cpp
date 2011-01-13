//==============================================================================
// Erlang bindings for ZeroMQ.
//
// Copyright (c) 2010
//
//  Dhammika Pathirana <dhammika at gmail dot com>
//     wrote original C code, copyright disclaimed.
//
//  Serge Aleynikov <saleyn at gmail dot com>
//     C++ rewrite, bug fixes and many enhancements to the driver to support
//     non-blocking I/O.
//
// Copyright (c) 2011
//
//  Chris Rempel <csrl at gmx dot com>
//     Wrapped each socket connection in a gen_server, fixed a bunch of bugs and
//     simplified code base
//
// See LICENSE for license details
//==============================================================================
#include "zmq_drv.h"

#include <errno.h>
#include <assert.h>

static ErlDrvTermData am_zmq_drv;
static ErlDrvTermData am_ok;
static ErlDrvTermData am_error;
static ErlDrvTermData am_true;
static ErlDrvTermData am_false;
static ErlDrvTermData am_pollin;
static ErlDrvTermData am_pollout;
static ErlDrvTermData am_pollerr;

static ErlDrvTermData am_ebusy;
static ErlDrvTermData am_enosys;
static ErlDrvTermData am_eterm;
static ErlDrvTermData am_efault;
static ErlDrvTermData am_einval;
static ErlDrvTermData am_eagain;
static ErlDrvTermData am_enotsup;
static ErlDrvTermData am_efsm;
static ErlDrvTermData am_emthread;
static ErlDrvTermData am_eprotonosupport;
static ErlDrvTermData am_enocompatproto;
static ErlDrvTermData am_eaddrinuse;
static ErlDrvTermData am_eaddrnotavail;
static ErlDrvTermData am_enodev;

//-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
// Internal Functions
//-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static ErlDrvTermData
error_atom(int err)
{
    switch (err)
    {
        case EBUSY:           return am_ebusy;
        case ENOSYS:          return am_enosys;
        case ETERM:           return am_eterm;
        case EFAULT:          return am_efault;
        case EINVAL:          return am_einval;
        case EAGAIN:          return am_eagain;
        case ENOTSUP:         return am_enotsup;
        case EFSM:            return am_efsm;
        case EMTHREAD:        return am_emthread;
        case EPROTONOSUPPORT: return am_eprotonosupport;
        case ENOCOMPATPROTO:  return am_enocompatproto;
        case EADDRINUSE:      return am_eaddrinuse;
        case EADDRNOTAVAIL:   return am_eaddrnotavail;
        case ENODEV:          return am_enodev;
        default:
            assert(true);
            return am_error; // Compiler complains otherwise.
    }
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
reply_error(ErlDrvPort port, ErlDrvTermData pid, int err)
{
    // Return {zmq, Socket::integer(), {error, Reason::atom()}}
    ErlDrvTermData spec[] = {
        ERL_DRV_ATOM,   am_zmq_drv,
        ERL_DRV_ATOM,   am_error,
        ERL_DRV_ATOM,   error_atom(err),
        ERL_DRV_TUPLE,  2,
        ERL_DRV_TUPLE,  2
    };
    driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
reply_ok(ErlDrvPort port, ErlDrvTermData pid)
{
    ErlDrvTermData spec[] = {
        ERL_DRV_ATOM,   am_zmq_drv,
        ERL_DRV_ATOM,   am_ok,
        ERL_DRV_TUPLE,  2
    };
    driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
reply_ok_binary(ErlDrvPort port, ErlDrvTermData pid, void *data, size_t size)
{
    /* Copy payload. */
    ErlDrvTermData spec[] = {
        ERL_DRV_ATOM,   am_zmq_drv,
        ERL_DRV_ATOM,   am_ok,
        ERL_DRV_BUF2BINARY, (ErlDrvTermData)data, size,
        ERL_DRV_TUPLE,  2,
        ERL_DRV_TUPLE,  2
    };
    driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
reply_ok_int(ErlDrvPort port, ErlDrvTermData pid, ErlDrvSInt num)
{
    ErlDrvTermData spec[] = {
        ERL_DRV_ATOM,   am_zmq_drv,
        ERL_DRV_ATOM,   am_ok,
        ERL_DRV_INT,    num,
        ERL_DRV_TUPLE,  2,
        ERL_DRV_TUPLE,  2
    };
    driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
reply_ok_int64(ErlDrvPort port, ErlDrvTermData pid, ErlDrvSInt64 num)
{
    ErlDrvTermData spec[] = {
        ERL_DRV_ATOM,   am_zmq_drv,
        ERL_DRV_ATOM,   am_ok,
        ERL_DRV_INT64,  TERM_DATA(&num),
        ERL_DRV_TUPLE,  2,
        ERL_DRV_TUPLE,  2
    };
    driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
reply_ok_uint64(ErlDrvPort port, ErlDrvTermData pid, ErlDrvUInt64 num)
{
    ErlDrvTermData spec[] = {
        ERL_DRV_ATOM,   am_zmq_drv,
        ERL_DRV_ATOM,   am_ok,
        ERL_DRV_UINT64, TERM_DATA(&num),
        ERL_DRV_TUPLE,  2,
        ERL_DRV_TUPLE,  2
    };
    driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
reply_ok_atom(ErlDrvPort port, ErlDrvTermData pid, ErlDrvTermData atom)
{
    ErlDrvTermData spec[] = {
        ERL_DRV_ATOM,   am_zmq_drv,
        ERL_DRV_ATOM,   am_ok,
        ERL_DRV_ATOM,   atom,
        ERL_DRV_TUPLE,  2,
        ERL_DRV_TUPLE,  2
    };
    driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
reply_ok_events(ErlDrvPort port, ErlDrvTermData pid, uint32_t events)
{
    if ((ZMQ_POLLIN|ZMQ_POLLOUT) == events)
    {
        ErlDrvTermData spec[] = {
            ERL_DRV_ATOM,   am_zmq_drv,
            ERL_DRV_ATOM,   am_ok,
            ERL_DRV_ATOM,   am_pollin,
            ERL_DRV_ATOM,   am_pollout,
            ERL_DRV_NIL,
            ERL_DRV_LIST,   3,
            ERL_DRV_TUPLE,  2,
            ERL_DRV_TUPLE,  2
        };
        driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
    }
    else
    {
        ErlDrvTermData spec[] = {
            ERL_DRV_ATOM,   am_zmq_drv,
            ERL_DRV_ATOM,   am_ok,
            ERL_DRV_ATOM,   am_pollerr,
            ERL_DRV_NIL,
            ERL_DRV_LIST,   2,
            ERL_DRV_TUPLE,  2,
            ERL_DRV_TUPLE,  2
        };
        switch (events)
        {
            case ZMQ_POLLIN:  spec[5] = am_pollin;  break;
            case ZMQ_POLLOUT: spec[5] = am_pollout; break;
        }
        driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
    }
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
send_events(ErlDrvPort port, ErlDrvTermData pid, uint32_t events)
{
    if ((ZMQ_POLLIN|ZMQ_POLLOUT) == events)
    {
        ErlDrvTermData spec[] = {
            ERL_DRV_ATOM,   am_zmq_drv,
            ERL_DRV_ATOM,   am_pollin,
            ERL_DRV_ATOM,   am_pollout,
            ERL_DRV_NIL,
            ERL_DRV_LIST,   3,
            ERL_DRV_TUPLE,  2
        };
        driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
    }
    else
    {
        ErlDrvTermData spec[] = {
            ERL_DRV_ATOM,   am_zmq_drv,
            ERL_DRV_ATOM,   am_pollerr,
            ERL_DRV_NIL,
            ERL_DRV_LIST,   2,
            ERL_DRV_TUPLE,  2
        };
        switch (events)
        {
            case ZMQ_POLLIN:  spec[3] = am_pollin;  break;
            case ZMQ_POLLOUT: spec[3] = am_pollout; break;
        }
        driver_send_term(port, pid, spec, sizeof(spec)/sizeof(spec[0]));
    }
}

//-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
// ZeroMQ Callback Functions
//-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
zmqcb_free_binary(void* /*data*/, void* hint)
{
    ErlDrvBinary* bin = (ErlDrvBinary*)hint;
    driver_free_binary(bin);
}

//-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
// ZeroMQ Wrapper Functions
//-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_init(zmq_drv_t *drv, const uint8_t* bytes, size_t size)
{
    int io_threads  = *bytes;

    assert(sizeof(uint8_t) == size);

    zmqdrv_fprintf("init (io_threads: %d)\r\n", io_threads);

    // We only support a single zmq context, but zeromq itself supports multiple
    if (drv->zmq_context)
    {
        reply_error(drv->port, driver_caller(drv->port), EBUSY);
        return;
    }

    drv->terminating = false;
    drv->zmq_context = zmq_init(io_threads);

    if (!drv->zmq_context)
    {
        reply_error(drv->port, driver_caller(drv->port), zmq_errno());
        return;
    }

    zmqdrv_fprintf("init %p\r\n", drv->zmq_context);

    reply_ok(drv->port, driver_caller(drv->port));
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_term(zmq_drv_t *drv)
{
    zmqdrv_fprintf("term %p\r\n", drv->zmq_context);

    if (0 < drv->zmq_pid_socket.size())
    {
        for (zmq_pid_socket_map_t::iterator it = drv->zmq_pid_socket.begin(); it != drv->zmq_pid_socket.end(); ++it)
        {
            zmq_sock_info* si = it->second;

            if (si->busy)
            {
                // Remove socket from erlang vm polling
                driver_select(drv->port, si->fd, ERL_DRV_READ, 0);

                if (si->out_caller)
                {
                    reply_error(drv->port, si->out_caller, ETERM);
                    si->out_caller = 0;
                    zmq_msg_close(&si->out_msg);
                }
                if (si->in_caller)
                {
                    reply_error(drv->port, si->in_caller, ETERM);
                    si->in_caller = 0;
                }
                if (si->poll_caller)
                {
                    send_events(drv->port, si->poll_caller, (uint32_t)ZMQ_POLLERR);
                    si->poll_caller = 0;
                }

                si->busy = false;
            }
        }

        // TODO: Remove if zeromq itself ever gets fixed. As zmq_term() is a
        // blocking call, and will not return until all sockets are closed,
        // so do not allow it to be called while there are open sockets.
        drv->terminating = true;
        reply_error(drv->port, driver_caller(drv->port), EAGAIN);
        return;
    }

    // cross fingers and hope zmq_term() doesn't block, else we hardlock.
    if (0 != zmq_term(drv->zmq_context))
    {
        reply_error(drv->port, driver_caller(drv->port), zmq_errno());
        return;
    }

    drv->zmq_context = NULL;

    reply_ok(drv->port, driver_caller(drv->port));
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_socket(zmq_drv_t *drv, const uint8_t* bytes, size_t size)
{
    int type = *bytes;

    assert(sizeof(uint8_t) == size);

    zmqdrv_fprintf("socket (type: %d)\r\n", type);

    ErlDrvTermData caller = driver_caller(drv->port);

    if (drv->terminating)
    {
        reply_error(drv->port, caller, ETERM);
        return;
    }

    assert(NULL == drv->get_socket_info(caller));

    void* s = zmq_socket(drv->zmq_context, type);

    if (!s)
    {
        reply_error(drv->port, caller, zmq_errno());
        return;
    }

    //TODO: Support Windows 'SOCKET' type?
    int fd; size_t fd_size = sizeof(fd);
    if (0 != zmq_getsockopt(s, ZMQ_FD, &fd, &fd_size))
    {
        reply_error(drv->port, caller, zmq_errno());
        zmq_close(s);
        return;
    }

    zmq_sock_info* si = new zmq_sock_info(s, (ErlDrvEvent)fd);

    if (!si)
    {
        driver_failure_posix(drv->port, ENOMEM);
        return;
    }

    driver_monitor_process(drv->port, caller, &si->monitor);
    drv->zmq_pid_socket[caller] = si;
    drv->zmq_fd_socket[si->fd] = si;

    zmqdrv_fprintf("socket %p owner %lu\r\n", si->socket, caller);

    reply_ok(drv->port, caller);
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_close(zmq_drv_t *drv)
{
    ErlDrvTermData caller = driver_caller(drv->port);
    zmq_sock_info* si = drv->get_socket_info(caller);

    if (!si)
    {
        reply_error(drv->port, caller, ENODEV);
        return;
    }

    zmqdrv_fprintf("close %p\r\n", si->socket);

    driver_demonitor_process(drv->port, &si->monitor);

    if (si->busy)
    {
        // Remove socket from vm polling
        driver_select(drv->port, si->fd, ERL_DRV_READ, 0);
    }

    drv->zmq_pid_socket.erase(caller);
    drv->zmq_fd_socket.erase(si->fd);

    //zmq_close(Socket) is called in ~zmq_sock_info
    delete si;

    reply_ok(drv->port, caller);
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_setsockopt(zmq_drv_t *drv, const uint8_t* bytes, size_t size)
{
    uint8_t        n = *bytes;
    const uint8_t* p =  bytes+sizeof(uint8_t);

    ErlDrvTermData caller = driver_caller(drv->port);

    if (drv->terminating)
    {
        reply_error(drv->port, caller, ETERM);
        return;
    }

    zmq_sock_info* si = drv->get_socket_info(caller);

    if (!si)
    {
        reply_error(drv->port, caller, ENODEV);
        return;
    }

    zmqdrv_fprintf("setsockopt %p (setting %d options)\r\n", si->socket, (int)n);

    for (uint8_t j = 0; j < n; ++j)
    {
        int         opt        = *p++;
        uint64_t    optvallen  = *p++;
        const void* optval     =  p;

        switch (opt)
        {
            case ZMQ_HWM:           assert(optvallen == sizeof(uint64_t));break;
            case ZMQ_SWAP:          assert(optvallen == sizeof(int64_t)); break;
            case ZMQ_AFFINITY:      assert(optvallen == sizeof(uint64_t));break;
            case ZMQ_IDENTITY:      assert(optvallen <= 255);             break;
            case ZMQ_SUBSCRIBE:     break; // While the erlang layer limits this to 255, there is no zmq limit
            case ZMQ_UNSUBSCRIBE:   break; // While the erlang layer limits this to 255, there is no zmq limit
            case ZMQ_RATE:          assert(optvallen == sizeof(int64_t)); break;
            case ZMQ_RECOVERY_IVL:  assert(optvallen == sizeof(int64_t)); break;
            case ZMQ_MCAST_LOOP:    assert(optvallen == sizeof(int64_t)); break;
            case ZMQ_SNDBUF:        assert(optvallen == sizeof(uint64_t));break;
            case ZMQ_RCVBUF:        assert(optvallen == sizeof(uint64_t));break;
            case ZMQ_LINGER:        assert(optvallen == sizeof(int));     break; // Erlang layer assumes 32bit
            case ZMQ_RECONNECT_IVL: assert(optvallen == sizeof(int));     break; // Erlang layer assumes 32bit
            case ZMQ_BACKLOG:       assert(optvallen == sizeof(int));     break; // Erlang layer assumes 32bit
            default:                assert(true);
        }

        assert((size_t)((uint8_t*)(p + optvallen) - bytes) <= size);

        zmqdrv_fprintf("setsockopt %p (opt: %d)\r\n", si->socket, opt);

        if (0 != zmq_setsockopt(si->socket, opt, optval, optvallen))
        {
            reply_error(drv->port, caller, zmq_errno());
            return;
        }

        p += optvallen;
    }

    reply_ok(drv->port, caller);
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_getsockopt(zmq_drv_t *drv, const uint8_t* bytes, size_t size)
{
    int opt = *bytes;

    assert(sizeof(uint8_t) == size);

    ErlDrvTermData caller = driver_caller(drv->port);

    if (drv->terminating)
    {
        reply_error(drv->port, caller, ETERM);
        return;
    }

    zmq_sock_info* si = drv->get_socket_info(caller);

    if (!si)
    {
        reply_error(drv->port, caller, ENODEV);
        return;
    }

    zmqdrv_fprintf("getsockopt %p (opt: %d)\r\n", si->socket, opt);

    uint8_t optval[255] = {0};
    size_t optvallen = 0;
    void * p = (void*)optval;

    switch (opt)
    {
        case ZMQ_HWM:           optvallen = sizeof(uint64_t); break;
        case ZMQ_SWAP:          optvallen = sizeof(int64_t);  break;
        case ZMQ_AFFINITY:      optvallen = sizeof(uint64_t); break;
        case ZMQ_IDENTITY:      optvallen = sizeof(optval);   break;
        case ZMQ_RATE:          optvallen = sizeof(int64_t);  break;
        case ZMQ_RECOVERY_IVL:  optvallen = sizeof(int64_t);  break;
        case ZMQ_MCAST_LOOP:    optvallen = sizeof(int64_t);  break;
        case ZMQ_SNDBUF:        optvallen = sizeof(uint64_t); break;
        case ZMQ_RCVBUF:        optvallen = sizeof(uint64_t); break;
        case ZMQ_RCVMORE:       optvallen = sizeof(int64_t);  break;
        case ZMQ_LINGER:        optvallen = sizeof(int);      break;
        case ZMQ_RECONNECT_IVL: optvallen = sizeof(int);      break;
        case ZMQ_BACKLOG:       optvallen = sizeof(int);      break;
        case ZMQ_FD:            optvallen = sizeof(int);      break;
        case ZMQ_EVENTS:        optvallen = sizeof(uint32_t); break;
        case ZMQ_TYPE:          optvallen = sizeof(int);      break;
        default:                assert(true);
    }

    if (0 != zmq_getsockopt(si->socket, opt, p, &optvallen))
    {
        reply_error(drv->port, caller, zmq_errno());
        return;
    }

    switch (opt)
    {
        // uint64
        case ZMQ_HWM:
        case ZMQ_AFFINITY:
        case ZMQ_SNDBUF:
        case ZMQ_RCVBUF:
            reply_ok_uint64(drv->port, caller, *(uint64_t*)p);
            break;

        // int64
        case ZMQ_SWAP:
        case ZMQ_RATE:
        case ZMQ_RECOVERY_IVL:
            reply_ok_int64(drv->port, caller, *(int64_t*)p);
            break;

        // bool (from int64 0/1)
        case ZMQ_RCVMORE:
        case ZMQ_MCAST_LOOP:
            reply_ok_atom(drv->port, caller, *(int64_t*)p ? am_true : am_false);
            break;

        // ZMQ_POLLIN|ZMQ_POLLOUT (from uint32)
        case ZMQ_EVENTS:
            reply_ok_events(drv->port, caller, *(uint32_t*)p);
            break;

        // int
        case ZMQ_LINGER:
        case ZMQ_RECONNECT_IVL:
        case ZMQ_BACKLOG:
        case ZMQ_FD:
        case ZMQ_TYPE:
            reply_ok_int(drv->port, caller, *(int*)p);
            break;

        // binary
        case ZMQ_IDENTITY:
            reply_ok_binary(drv->port, caller, p, optvallen);
            break;
    }
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_bind(zmq_drv_t *drv, const uint8_t* bytes, size_t size)
{
    // expects the address to be zero terminated
    char* addr  = (char*)bytes;

    // TODO: check for zero termination within size limit
    assert(sizeof(char) <= size); // Must always have at least the 0 terminating char.

    ErlDrvTermData caller = driver_caller(drv->port);

    if (drv->terminating)
    {
        reply_error(drv->port, caller, ETERM);
        return;
    }

    zmq_sock_info* si = drv->get_socket_info(caller);

    if (!si)
    {
        reply_error(drv->port, caller, ENODEV);
        return;
    }

    zmqdrv_fprintf("bind %p (addr: %s)\r\n", si->socket, addr);

    if (0 != zmq_bind(si->socket, addr))
    {
        reply_error(drv->port, caller, zmq_errno());
        return;
    }

    reply_ok(drv->port, caller);
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_connect(zmq_drv_t *drv, const uint8_t* bytes, size_t size)
{
    // expects the address to be zero terminated
    char* addr  = (char*)bytes;

    // TODO: check for zero termination within size limit
    assert(sizeof(char) <= size); // Must always have at least the 0 terminating char.

    ErlDrvTermData caller = driver_caller(drv->port);

    if (drv->terminating)
    {
        reply_error(drv->port, caller, ETERM);
        return;
    }

    zmq_sock_info* si = drv->get_socket_info(caller);

    if (!si)
    {
        reply_error(drv->port, caller, ENODEV);
        return;
    }

    zmqdrv_fprintf("connect %p (addr: %s)\r\n", si->socket, addr);

    if (0 != zmq_connect(si->socket, addr))
    {
        reply_error(drv->port, caller, zmq_errno());
        return;
    }

    reply_ok(drv->port, caller);
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_send(zmq_drv_t *drv, const uint8_t* bytes, size_t size, ErlDrvBinary* bin)
{
    int     flags     = *bytes;
    void*   data      = (void *)(bytes+sizeof(uint8_t));
    size_t  data_size = size - sizeof(uint8_t);

    assert(sizeof(uint8_t) <= size);

    ErlDrvTermData caller = driver_caller(drv->port);

    if (drv->terminating)
    {
        reply_error(drv->port, caller, ETERM);
        return;
    }

    zmq_sock_info* si = drv->get_socket_info(caller);

    if (!si)
    {
        reply_error(drv->port, caller, ENODEV);
        return;
    }

    assert(0 == si->out_caller);

    zmqdrv_fprintf("send %p (flags: %d bytes: %u)\r\n", si->socket, flags, data_size);

    if (si->out_caller || si->in_caller)
    {
        // There's still a blocking send/recv pending
        reply_error(drv->port, caller, EBUSY);
        return;
    }

    // Increment the reference count on binary so that zmq can take ownership of it.
    driver_binary_inc_refc(bin);

    if (zmq_msg_init_data(&si->out_msg, data, data_size, &zmqcb_free_binary, bin))
    {
        reply_error(drv->port, caller, zmq_errno());
        driver_binary_dec_refc(bin);
        return;
    }

    if (0 == zmq_send(si->socket, &si->out_msg, flags|ZMQ_NOBLOCK))
    {
        reply_ok(drv->port, caller);
        zmq_msg_close(&si->out_msg);
    }
    else if (ZMQ_NOBLOCK != (ZMQ_NOBLOCK & flags) && EAGAIN == zmq_errno())
    {
        // Caller requested blocking send
        // Can't send right now. Make the caller wait by not returning result
        zmqdrv_fprintf("send %p blocking\r\n", si->socket);

        si->out_flags = flags;
        si->out_caller = caller;

        if (!si->busy)
        {
            driver_select(drv->port, si->fd, ERL_DRV_READ, 1);
            si->busy = true;
        }
    }
    else
    {
        reply_error(drv->port, caller, zmq_errno());
        zmq_msg_close(&si->out_msg);
    }
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_recv(zmq_drv_t *drv, const uint8_t* bytes, size_t size)
{
    int flags = *bytes;

    assert(sizeof(uint8_t) == size);

    ErlDrvTermData caller = driver_caller(drv->port);

    if (drv->terminating)
    {
        reply_error(drv->port, caller, ETERM);
        return;
    }

    zmq_sock_info* si = drv->get_socket_info(caller);

    if (!si)
    {
        reply_error(drv->port, caller, ENODEV);
        return;
    }

    assert(0 == si->in_caller);

    zmqdrv_fprintf("recv %p (flags: %d)\r\n", si->socket, flags);

    zmq_msg_t msg;
    zmq_msg_init(&msg);

    if (0 == zmq_recv(si->socket, &msg, flags|ZMQ_NOBLOCK))
    {
        reply_ok_binary(drv->port, caller, zmq_msg_data(&msg), zmq_msg_size(&msg));
    }
    else if (ZMQ_NOBLOCK != (ZMQ_NOBLOCK & flags) && EAGAIN == zmq_errno())
    {
        // Caller requested blocking recv
        // No input available. Make the caller wait by not returning result
        zmqdrv_fprintf("recv %p blocking\r\n", si->socket);

        si->in_flags = flags;
        si->in_caller = caller;

        if (!si->busy)
        {
            driver_select(drv->port, si->fd, ERL_DRV_READ, 1);
            si->busy = true;
        }
    }
    else
    {
        reply_error(drv->port, caller, zmq_errno());
    }

    zmq_msg_close(&msg);
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
wrap_zmq_poll(zmq_drv_t *drv, const uint8_t* bytes, size_t size)
{
    uint32_t events = *bytes;

    assert(sizeof(uint8_t) == size);

    ErlDrvTermData caller = driver_caller(drv->port);

    if (drv->terminating)
    {
        reply_error(drv->port, caller, ETERM);
        return;
    }

    zmq_sock_info* si = drv->get_socket_info(caller);

    if (!si)
    {
        reply_error(drv->port, caller, ENODEV);
        return;
    }

    zmqdrv_fprintf("poll %p (events: %u)\r\n", si->socket, events);

    if (si->busy)
    {
        reply_error(drv->port, caller, EBUSY);
        return;
    }

    assert((ZMQ_POLLIN|ZMQ_POLLOUT) & events);

    uint32_t revents;
    size_t revents_size = sizeof(revents);

    if (0 == zmq_getsockopt(si->socket, ZMQ_EVENTS, &revents, &revents_size))
    {
        // reply immediately; poll event notification happens out of band.
        reply_ok(drv->port, caller);

        revents &= events;

        if (0 != revents)
        {
            send_events(drv->port, caller, revents);
        }
        else
        {
            // No matching pending event, wait for one.
            zmqdrv_fprintf("poll %p blocking\r\n", si->socket);

            si->poll_events = events;
            si->poll_caller = caller;
            si->busy = true;
            driver_select(drv->port, si->fd, ERL_DRV_READ, 1);
        }
    }
    else
    {
        // EINVAL will only occur if our getsockopt call was invalid
        assert(EINVAL != zmq_errno());

        // else the problem was with the context/socket, and should be returned
        reply_error(drv->port, caller, zmq_errno());
    }
}

//-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
// Driver callbacks
//-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static int
zmqdrv_driver_init(void)
{
    INIT_ATOM(zmq_drv);
    INIT_ATOM(ok);
    INIT_ATOM(error);
    INIT_ATOM(true);
    INIT_ATOM(false);
    INIT_ATOM(pollin);
    INIT_ATOM(pollout);
    INIT_ATOM(pollerr);

    INIT_ATOM(ebusy);
    INIT_ATOM(enosys);
    INIT_ATOM(eterm);
    INIT_ATOM(efault);
    INIT_ATOM(einval);
    INIT_ATOM(eagain);
    INIT_ATOM(enotsup);
    INIT_ATOM(efsm);
    INIT_ATOM(emthread);
    INIT_ATOM(eprotonosupport);
    INIT_ATOM(enocompatproto);
    INIT_ATOM(eaddrinuse);
    INIT_ATOM(eaddrnotavail);
    INIT_ATOM(enodev);

    return 0;
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static ErlDrvData
zmqdrv_start(ErlDrvPort port, char* /*cmd*/)
{
    zmqdrv_fprintf("zmq port driver started by pid %lu\r\n", driver_connected(port));
    return reinterpret_cast<ErlDrvData>(new zmq_drv_t(port));
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
zmqdrv_stop(ErlDrvData handle)
{
    zmqdrv_fprintf("zmq port driver stopping\r\n");
    delete reinterpret_cast<zmq_drv_t*>(handle);
    zmqdrv_fprintf("zmq port driver stopped\r\n");
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
zmqdrv_ready_input(ErlDrvData handle, ErlDrvEvent event)
{
    zmq_drv_t *drv = reinterpret_cast<zmq_drv_t*>(handle);
    zmq_sock_info *si = drv->get_socket_info(event);

    // I'm not sure if a race condition could develop here or not
    // So let's assert and see if we ever hit it.  Hopefully not.
    assert(!drv->terminating);
    assert(NULL != si);
    assert(si->busy);

    // unregister event with erlang vm while we work with the socket
    driver_select(drv->port, si->fd, ERL_DRV_READ, 0);

    // Finish blocking recv request if input is ready
    if (si->in_caller)
    {
        zmq_msg_t msg;
        zmq_msg_init(&msg);

        if (0 == zmq_recv(si->socket, &msg, si->in_flags|ZMQ_NOBLOCK))
        {
            // Unblock the waiting caller's pid by returning result
            reply_ok_binary(drv->port, si->in_caller, zmq_msg_data(&msg), zmq_msg_size(&msg));
            si->in_caller = 0;
            si->in_flags = 0;
        }
        else if (zmq_errno() != EAGAIN)
        {
            // Unblock the waiting caller's pid by returning error
            reply_error(drv->port, si->in_caller, zmq_errno());
            si->in_caller = 0;
            si->in_flags = 0;
        }
        // else no input was ready, continue waiting

        zmq_msg_close(&msg);
    }

    // Finish blocking send request if able
    if (si->out_caller)
    {
        if (0 == zmq_send(si->socket, &si->out_msg, si->out_flags|ZMQ_NOBLOCK))
        {
            // Unblock the waiting caller's pid by returning result
            reply_ok(drv->port, si->out_caller);
            si->out_caller = 0;
            si->out_flags = 0;
            zmq_msg_close(&si->out_msg);
        }
        else if (zmq_errno() != EAGAIN)
        {
            // Unblock the waiting caller's pid by returning error
            reply_error(drv->port, si->out_caller, zmq_errno());
            si->out_caller = 0;
            si->out_flags = 0;
            zmq_msg_close(&si->out_msg);
        }
        // else not able to send, continue waiting
    }

    // Finish poll request if events available
    if (si->poll_caller)
    {
        uint32_t revents = 0;
        size_t revents_size = sizeof(revents);

        if (0 == zmq_getsockopt(si->socket, ZMQ_EVENTS, &revents, &revents_size))
        {
            revents &= si->poll_events;

            if (0 != revents)
            {
                send_events(drv->port, si->poll_caller, revents);
                si->poll_caller = 0;
                si->poll_events = 0;
            }
            // else no requested events pending, continue waiting
        }
        else
        {
            // EINVAL will only occur if our getsockopt call was invalid
            assert(EINVAL != zmq_errno());

            // send out of band event error notification
            send_events(drv->port, si->poll_caller, (uint32_t)ZMQ_POLLERR);
            si->poll_caller = 0;
            si->poll_events = 0;
        }
    }

    // reregister event with erlang vm if any pending operations exist
    if (si->poll_caller || si->in_caller || si->out_caller)
    {
        driver_select(drv->port, si->fd, ERL_DRV_READ, 1);
    }
    else
    {
        si->busy = false;
    }
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
zmqdrv_process_exit(ErlDrvData handle, ErlDrvMonitor* monitor)
{
    zmq_drv_t*     drv = reinterpret_cast<zmq_drv_t*>(handle);
    ErlDrvTermData pid = driver_get_monitored_process(drv->port, monitor);

    zmqdrv_fprintf("detected death of %lu process\r\n", pid);

    zmq_sock_info* si = drv->get_socket_info(pid);

    assert(NULL != si);

    zmqdrv_fprintf("force close %p\r\n", si->socket);

    driver_demonitor_process(drv->port, &si->monitor);

    if (si->busy)
    {
        // Remove socket from vm polling
        driver_select(drv->port, si->fd, ERL_DRV_READ, 0);
    }

    drv->zmq_pid_socket.erase(pid);
    drv->zmq_fd_socket.erase(si->fd);

    //zmq_close(Socket) is called in ~zmq_sock_info
    delete si;
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static void
zmqdrv_outputv(ErlDrvData handle, ErlIOVec *ev)
{
    zmq_drv_t*            drv   = reinterpret_cast<zmq_drv_t*>(handle);
    ErlDrvBinary*         bin   = ev->binv[1];
    enum driver_commands  cmd   = (enum driver_commands)bin->orig_bytes[0];
    uint8_t*              bytes = (uint8_t*)&bin->orig_bytes[1];

    assert(1 <= bin->orig_size);

    zmqdrv_fprintf("driver got command %d on thread %p\r\n", (int)cmd, erl_drv_thread_self());

    switch (cmd)
    {
        case ZMQ_INIT:
            wrap_zmq_init(drv, bytes, bin->orig_size - 1);
            break;
        case ZMQ_TERM:
            wrap_zmq_term(drv);
            break;
        case ZMQ_SOCKET:
            wrap_zmq_socket(drv, bytes, bin->orig_size - 1);
            break;
        case ZMQ_CLOSE:
            wrap_zmq_close(drv);
            break;
        case ZMQ_SETSOCKOPT:
            wrap_zmq_setsockopt(drv, bytes, bin->orig_size - 1);
            break;
        case ZMQ_GETSOCKOPT:
            wrap_zmq_getsockopt(drv, bytes, bin->orig_size - 1);
            break;
        case ZMQ_BIND:
            wrap_zmq_bind(drv, bytes, bin->orig_size - 1);
            break;
        case ZMQ_CONNECT:
            wrap_zmq_connect(drv, bytes, bin->orig_size - 1);
            break;
        case ZMQ_SEND:
            wrap_zmq_send(drv, bytes, bin->orig_size - 1, bin);
            break;
        case ZMQ_RECV:
            wrap_zmq_recv(drv, bytes, bin->orig_size - 1);
            break;
        case ZMQ_POLL:
            wrap_zmq_poll(drv, bytes, bin->orig_size - 1);
            break;
        default :
            assert(true);
    }
}

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
static ErlDrvEntry zmq_driver_entry = {
    zmqdrv_driver_init,                 // init
    zmqdrv_start,                       // startup
    zmqdrv_stop,                        // shutdown
    NULL,                               // output
    zmqdrv_ready_input,                 // ready_input
    NULL,                               // ready_output
    (char*)"zmq_drv",                   // driver name
    NULL,                               // finish
    NULL,                               // handle
    NULL,                               // control
    NULL,                               // timeout
    zmqdrv_outputv,                     // outputv, binary output
    NULL,                               // ready_async
    NULL,                               // flush
    NULL,                               // call
    NULL,                               // event
    ERL_DRV_EXTENDED_MARKER,            // ERL_DRV_EXTENDED_MARKER
    ERL_DRV_EXTENDED_MAJOR_VERSION,     // ERL_DRV_EXTENDED_MAJOR_VERSION
    ERL_DRV_EXTENDED_MAJOR_VERSION,     // ERL_DRV_EXTENDED_MINOR_VERSION
    ERL_DRV_FLAG_USE_PORT_LOCKING,      // ERL_DRV_FLAGs
    NULL,                               // handle2 (reserved)
    zmqdrv_process_exit,                // process_exit
    NULL                                // stop_select
};

//-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
extern "C"
DRIVER_INIT(zmq_drv)
{
    return &zmq_driver_entry;
}
