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
#include <erl_driver.h>
#include <zmq.h>
#include <stdint.h>
#include <map>

#ifdef ZMQDRV_DEBUG
#include <stdio.h>
#define zmqdrv_fprintf(format, ...)   fprintf(stdout, "["__FILE__":%d] " format, __LINE__, ##__VA_ARGS__)
#else
#define zmqdrv_fprintf(...)
#endif

#define INIT_ATOM(NAME) am_ ## NAME = (ErlDrvTermData)driver_mk_atom((char*)#NAME)

enum driver_commands
{
     ZMQ_INIT = 1
    ,ZMQ_TERM
    ,ZMQ_SOCKET
    ,ZMQ_CLOSE
    ,ZMQ_SETSOCKOPT
    ,ZMQ_GETSOCKOPT
    ,ZMQ_BIND
    ,ZMQ_CONNECT
    ,ZMQ_SEND
    ,ZMQ_RECV
    ,ZMQ_POLL
};

struct zmq_sock_info
{
    void*           socket;       // 0MQ socket handle
    ErlDrvEvent     fd;           // Signaling fd for this socket
    ErlDrvMonitor   monitor;      // Owner process monitor
    int             in_flags;     // Read flags for the pending request
    ErlDrvTermData  in_caller;    // Caller's pid of the last blocking recv() command
    zmq_msg_t       out_msg;      // Pending message to be written to 0MQ socket
    int             out_flags;    // Send flags for the pending message
    ErlDrvTermData  out_caller;   // Caller's pid of the last blocking send() command
    uint32_t        poll_events;  // ZMQ_POLLIN | ZMQ_POLLOUT
    ErlDrvTermData  poll_caller;  // Caller's pid of the last poll() command
    bool            busy;         // true if port has a pending operation, else false

    zmq_sock_info(void* _s, ErlDrvEvent _fd)
        :socket(_s), fd(_fd)
        ,in_flags(0), in_caller(0)
        ,out_flags(0), out_caller(0)
        ,poll_events(0), poll_caller(0)
        ,busy(false)
    {}

    ~zmq_sock_info()
    {
        if (out_caller) zmq_msg_close(&out_msg);
        if (socket) zmq_close(socket);
    }

    static void* operator new    (size_t sz) { return driver_alloc(sz); }
    static void  operator delete (void* p)   { driver_free(p); }
};

typedef std::map<ErlDrvEvent,     zmq_sock_info*>     zmq_fd_socket_map_t;
typedef std::map<ErlDrvTermData,  zmq_sock_info*>     zmq_pid_socket_map_t;

// Driver state structure
struct zmq_drv_t
{
    ErlDrvPort            port;           // Erlang Port
    void*                 zmq_context;    // ZMQ Context
    bool                  terminating;    // Set if zmq_term() has been called
    zmq_fd_socket_map_t   zmq_fd_socket;  // ZMQ FD to Socket mapping
    zmq_pid_socket_map_t  zmq_pid_socket; // Owner Process to Socket mapping

    zmq_drv_t(ErlDrvPort _port)
        : port(_port), zmq_context(NULL), terminating(true)
    {}

    ~zmq_drv_t()
    {
        for (zmq_pid_socket_map_t::iterator it = zmq_pid_socket.begin(); it != zmq_pid_socket.end(); ++it)
        {
            zmq_sock_info* si = it->second;

            driver_demonitor_process(port, &si->monitor);

            if (si->busy)
            {
                // Remove socket from erlang vm polling
                driver_select(port, si->fd, ERL_DRV_READ, 0);
            }

            delete si;
        }

        zmq_pid_socket.clear();
        zmq_fd_socket.clear();

        if (zmq_context)
        {
            zmq_term(zmq_context);
        }
    }

    zmq_sock_info* get_socket_info(ErlDrvTermData pid)
    {
        zmq_pid_socket_map_t::const_iterator it = zmq_pid_socket.find(pid);
        return it == zmq_pid_socket.end() ? NULL : it->second;
    }

    zmq_sock_info* get_socket_info(ErlDrvEvent fd)
    {
        zmq_fd_socket_map_t::const_iterator it = zmq_fd_socket.find(fd);
        return it == zmq_fd_socket.end() ? NULL : it->second;
    }

    static void* operator new    (size_t sz) { return driver_alloc(sz); }
    static void  operator delete (void* p)   { driver_free(p); }
};
