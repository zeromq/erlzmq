/*
 * ------------------------------------------------------------------
 * Erlang bindings for ZeroMQ.
 * ------------------------------------------------------------------
 * Copyright (c) 2010 Dhammika Pathirana and Serge Aleynikov
 * <dhammika@gmail.com> wrote original C code, copyright disclaimed.
 * <saleyn@gmail.com> C++ rewrite, bug fixes and many enhancements
 * to the driver to support non-blocking I/O.
 * ------------------------------------------------------------------
 * See ../LICENSE for license details
 */

// uncomment the next line to enable debug
//#define ZMQDRV_DEBUG
// uncomment the next line to output debug info to a file of the format zmq_drv.<pid>.log
// useful for testing the zmq primitives in the erl standalone shell
//#define ZMQDRV_DEBUG_TO_FILE

#include <stdio.h>
#include <string.h>
#include <zmq.h>
#include <ctype.h>
#include <sstream>
#include <assert.h>
// in windows process.h is needed for the getpid method to append to the log file
#if defined(__WIN32__) && defined(ZMQDRV_DEBUG) && defined(ZMQDRV_DEBUG_TO_FILE)
#	include <process.h>
#endif
#include "zmq_drv.h"

#ifdef ZMQDRV_DEBUG
#	ifdef ZMQDRV_DEBUG_TO_FILE
#		ifdef __WIN32__
// in windows getpid is deemed unsafe, must use _getpid instead
#			define getpid _getpid
#	endif
		static FILE *debug_file;
#		define zmqdrv_fprintf(...)   fprintf(debug_file, __VA_ARGS__);fflush(debug_file);
#	else
#		define zmqdrv_fprintf(...)   fprintf(stderr, __VA_ARGS__)
#	endif
#else
#	define zmqdrv_fprintf(...)
#endif

#define INIT_ATOM(NAME) am_ ## NAME = (ErlDrvTermData)driver_mk_atom((char*)#NAME)

/* Callbacks */
static ErlDrvEntry zmq_driver_entry = {
    zmqdrv_driver_init,                 /* init */
    zmqdrv_start,                       /* startup (defined below) */
    zmqdrv_stop,                        /* shutdown (defined below) */
    NULL,                               /* output */
    zmqdrv_ready_input,                 /* ready_input */
    NULL,                               /* ready_output */
    (char*)"zmq_drv",                   /* driver name */
    NULL,                               /* finish */
    NULL,                               /* handle */
    NULL,                               /* control */
    NULL,                               /* timeout */
    zmqdrv_outputv,                     /* outputv, binary output */
    NULL,                               /* ready_async */
    NULL,                               /* flush */
    NULL,                               /* call */
    NULL,                               /* event */
    ERL_DRV_EXTENDED_MARKER,            /* ERL_DRV_EXTENDED_MARKER */
    ERL_DRV_EXTENDED_MAJOR_VERSION,     /* ERL_DRV_EXTENDED_MAJOR_VERSION */
    ERL_DRV_EXTENDED_MINOR_VERSION,     /* ERL_DRV_EXTENDED_MINOR_VERSION */
    ERL_DRV_FLAG_USE_PORT_LOCKING,      /* ERL_DRV_FLAGs */
    NULL,                               /* handle2 (reserved */
    zmqdrv_process_exit,                /* process_exit */
    NULL                                /* stop_select */
};

/* Driver internal, C hook to driver API. */
extern "C"
{
	DRIVER_INIT(zmq_drv)
	{
		return &zmq_driver_entry;
	}
}

zmq_drv_t::~zmq_drv_t()
{
    zmqdrv_fprintf("[zmq_drv] destroying zmq_drv_t object\r\n");

    for (zmq_pid_sockets_map_t::iterator it = zmq_pid_sockets.begin();
            it != zmq_pid_sockets.end(); ++it)
        driver_demonitor_process(port, &it->second.monitor);

#ifdef __WIN32__
	// on win32 we unregister wsa events, not socket descriptors
    for (zmq_wsa_events_map_t::iterator it = zmq_wsa_events.begin();
            it != zmq_wsa_events.end(); ++it) {
        driver_select(port, (ErlDrvEvent)it->first, ERL_DRV_READ, 0);
        zmqdrv_fprintf("[zmq_drv] unregistered wsa event %d with Erlang VM\r\n", it->first);
	}
#else
    for (zmq_fd_sockets_map_t::iterator it = zmq_fd_sockets.begin();
            it != zmq_fd_sockets.end(); ++it) {
        driver_select(port, (ErlDrvEvent)it->first, ERL_DRV_READ, 0);
        zmqdrv_fprintf("[zmq_drv] unregistered socket descriptor %d with Erlang VM\r\n", it->first);
	}
#endif

    for (zmq_sock_info *it = zmq_sock_infos, *next = (it ? it->next : NULL); it; it = next) {
        next = it->next;
        delete (&*it);
    }
    zmq_sockets.clear();
    zmq_idxs.clear();
    zmq_pid_sockets.clear();
    zmq_fd_sockets.clear();

    if (zmq_context) {
        zmqdrv_fprintf("[zmq_drv] calling zmq_term(context)\r\n");
        zmq_term(zmq_context);
        zmqdrv_fprintf("[zmq_drv] terminated zmq context\r\n");
    }
}

void zmq_drv_t::add_socket(zmq_sock_info* s)
{
    // Insert the new socket info to the head of the list
    if (zmq_sock_infos) zmq_sock_infos->prev = s;
    s->next        = zmq_sock_infos;
    zmq_sock_infos = s;

    // Update map: idx -> socket
    zmq_sockets[s->idx] = s;
    // Update map: socket -> idx
    zmq_idxs[s->socket] = s;
    {
        // Update map: pid -> sockets
        zmq_pid_sockets_map_t::iterator it = zmq_pid_sockets.find(s->owner);
        if (it != zmq_pid_sockets.end())
            it->second.sockets.insert(s);
        else {
            monitor_sockets_t ms;
            driver_monitor_process(port, s->owner, &ms.monitor);
            ms.sockets.insert(s);
            zmq_pid_sockets[s->owner] = ms;
        }
    }
    {
        // Update map: fd -> sockets
        zmq_fd_sockets_map_t::iterator it = zmq_fd_sockets.find(s->fd);
        if (it != zmq_fd_sockets.end())
            it->second.insert(s);
        else {
            zmq_sock_set_t set;
            set.insert(s);
            zmq_fd_sockets[s->fd] = set;
#ifdef __WIN32__
			zmq_wsa_events[s->wsa_event] = set;
#endif
        }
    }
}

int zmq_drv_t::del_socket(uint32_t idx, bool erase_from_list)
{
    zmq_sock_info* s;

    zmq_idx_socket_map_t::iterator it = zmq_sockets.find(idx);
    if (it == zmq_sockets.end()) {
		zmqdrv_fprintf("[zmq_drv] warning: socket info not found for #%d\r\n", idx);
        return -1;
    }

    s = it->second;
    s->unlink();
    if (s == zmq_sock_infos)
        zmq_sock_infos = s->next;

	zmqdrv_fprintf("[zmq_drv] deleting socket %d with index #%d\r\n", s->fd, idx);

    zmq_sockets.erase(idx);
    zmq_idxs.erase(s->socket);

    {
        // Remove the socket from a list of sockets owned by pid.
        // If this was the last socket, demonitor pid.
        zmq_pid_sockets_map_t::iterator it = zmq_pid_sockets.find(s->owner);
        if (it != zmq_pid_sockets.end()) {
			if (erase_from_list) {
				zmqdrv_fprintf("[zmq_drv] erased zmq_sock_info %p from monitored sockets list\r\n", s);
				it->second.sockets.erase(s);
			}
			// if it was the last one then end monitoring for the process
            if (it->second.sockets.empty()) {
				zmqdrv_fprintf("[zmq_drv] last socket erased, demonitoring process\r\n");
                driver_demonitor_process(port, &it->second.monitor);
                zmq_pid_sockets.erase(it);
            }
        }
    }
    {
        zmq_fd_sockets_map_t::iterator it = zmq_fd_sockets.find(s->fd);
        if (it != zmq_fd_sockets.end()) {
            it->second.erase(s);
            if (it->second.empty()) {
                zmq_fd_sockets.erase(it->first);
#ifdef __WIN32__
				zmq_wsa_events.erase(s->wsa_event);
				driver_select(port, (ErlDrvEvent) s->wsa_event, ERL_DRV_READ, 0);
				zmqdrv_fprintf("[zmq_drv] unregistered wsa event %d associated with socket %d on Erlang VM\r\n", s->wsa_event, s->fd);
#else
				driver_select(port, (ErlDrvEvent) s->fd, ERL_DRV_READ, 0);
				zmqdrv_fprintf("[zmq_drv] unregistered socket descriptor %d with Erlang VM\r\n", s->fd);
#endif        
            }
        }
    }

    delete s;
    return 0;
}

uint32_t zmq_drv_t::get_socket_idx(zmq_socket_t sock) const
{
    zmq_socket_idx_map_t::const_iterator it = zmq_idxs.find(sock);
	uint32_t idx = (it == zmq_idxs.end() ? 0 : it->second->idx);
	zmqdrv_fprintf("[zmq_drv] zmq socket %p(#%d) requested\r\n", sock, idx);
    return idx;
}

zmq_sock_info* zmq_drv_t::get_socket_info(uint32_t idx)
{
    zmq_idx_socket_map_t::const_iterator it = zmq_sockets.find(idx);
    return it == zmq_sockets.end() ? NULL : it->second;
}

zmq_socket_t zmq_drv_t::get_zmq_socket(uint32_t idx) const
{
	zmqdrv_fprintf("[zmq_drv] zmq socket #%d requested\r\n", idx);
    zmq_idx_socket_map_t::const_iterator it = zmq_sockets.find(idx);
	zmq_socket_t zmq_sock = (it == zmq_sockets.end() ? NULL : it->second->socket);
	zmqdrv_fprintf("[zmq_drv] zmq socket %p(#%d) requested\r\n", zmq_sock, idx);
    return zmq_sock;
}

// the next debug methods are not needed if not in debug
#ifdef ZMQDRV_DEBUG
static char *
debug_driver_command(driver_commands command)
{
    switch (command) {
        case ZMQ_INIT:			return (char*)"ZMQ_INIT";
        case ZMQ_TERM:			return (char*)"ZMQ_TERM";
        case ZMQ_SOCKET:		return (char*)"ZMQ_SOCKET";
        case ZMQ_CLOSE:			return (char*)"ZMQ_CLOSE";
        case ZMQ_SETSOCKOPT:	return (char*)"ZMQ_SETSOCKOPT";
        case ZMQ_GETSOCKOPT:	return (char*)"ZMQ_GETSOCKOPT";
        case ZMQ_BIND:			return (char*)"ZMQ_BIND";
        case ZMQ_CONNECT:		return (char*)"ZMQ_CONNECT";
        case ZMQ_SEND:			return (char*)"ZMQ_SEND";
        case ZMQ_RECV:			return (char*)"ZMQ_RECV";
        case ZMQ_FLUSH:			return (char*)"ZMQ_FLUSH";
        default:				return (char*)"unknown";
    }
}

static char *
debug_zmq_socket_option(int option)
{
    switch (option) {
        case ZMQ_HWM:           return (char*)"ZMQ_HWM";
        case ZMQ_SWAP:          return (char*)"ZMQ_SWAP";
        case ZMQ_AFFINITY:      return (char*)"ZMQ_AFFINITY";
        case ZMQ_IDENTITY:      return (char*)"ZMQ_IDENTITY";
        case ZMQ_SUBSCRIBE:     return (char*)"ZMQ_SUBSCRIBE";
        case ZMQ_UNSUBSCRIBE:   return (char*)"ZMQ_UNSUBSCRIBE";
        case ZMQ_RATE:          return (char*)"ZMQ_RATE";
        case ZMQ_RECOVERY_IVL:  return (char*)"ZMQ_RECOVERY_IVL";
        case ZMQ_MCAST_LOOP:    return (char*)"ZMQ_MCAST_LOOP";
        case ZMQ_SNDBUF:        return (char*)"ZMQ_SNDBUF";
        case ZMQ_RCVBUF:        return (char*)"ZMQ_RCVBUF";
        case ZMQ_ACTIVE:        return (char*)"ZMQ_ACTIVE";
		default:				return (char*)"unknown";
    }
}
#endif

static ErlDrvTermData error_atom(int err)
{
    char errstr[128];
    char* s;
    char* t;

    switch (err) {
        case ENOTSUP:           strcpy(errstr, "enotsup");          break;
        case EPROTONOSUPPORT:   strcpy(errstr, "eprotonosupport");  break;
        case ENOBUFS:           strcpy(errstr, "enobufs");          break;
        case ENETDOWN:          strcpy(errstr, "enetdown");         break;
        case EADDRINUSE:        strcpy(errstr, "eaddrinuse");       break;
        case EADDRNOTAVAIL:     strcpy(errstr, "eaddrnotavail");    break;
        case ECONNREFUSED:      strcpy(errstr, "econnrefused");     break;
        case EINPROGRESS:       strcpy(errstr, "einprogress");      break;
        case EFSM:              strcpy(errstr, "efsm");             break;
        case ENOCOMPATPROTO:    strcpy(errstr, "enocompatproto");   break;
        default:
            for (s = erl_errno_id(err), t = errstr; *s; s++, t++)
                *t = tolower(*s);
            *t = '\0';
    }
    return driver_mk_atom(errstr);
}

static void
zmq_free_binary(void* /*data*/, void* hint)
{
    ErlDrvBinary* bin = (ErlDrvBinary*)hint;
    driver_free_binary(bin);
}

static void
zmqdrv_socket_error(zmq_drv_t *drv, ErlDrvTermData pid, uint32_t idx, int err) {
    // Return {zmq, Socket::integer(), {error, Reason::atom()}}
    ErlDrvTermData spec[] =
        {ERL_DRV_ATOM,  am_zmq,
            ERL_DRV_UINT,  idx,
            ERL_DRV_ATOM,  am_error,
            ERL_DRV_ATOM,  error_atom(err),
            ERL_DRV_TUPLE, 2,
            ERL_DRV_TUPLE, 3};
    driver_send_term(drv->port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

static void
zmqdrv_error(zmq_drv_t *drv, const char *errstr)
{
    ErlDrvTermData spec[] =
        {ERL_DRV_ATOM,   am_error,
         ERL_DRV_STRING, (ErlDrvTermData)errstr, strlen(errstr),
         ERL_DRV_TUPLE,  2};
    driver_send_term(drv->port, driver_caller(drv->port), spec, sizeof(spec)/sizeof(spec[0]));
}

static void
zmqdrv_error_code(zmq_drv_t *drv, int err)
{
    ErlDrvTermData spec[] =
        {ERL_DRV_ATOM, am_error,
         ERL_DRV_ATOM, error_atom(err),
         ERL_DRV_TUPLE,  2};
    driver_send_term(drv->port, driver_caller(drv->port), spec, sizeof(spec)/sizeof(spec[0]));
}

static void
zmqdrv_ok(zmq_drv_t *drv, ErlDrvTermData pid)
{
    ErlDrvTermData spec[] = {ERL_DRV_ATOM, am_zok};
    driver_send_term(drv->port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

static void
zmqdrv_ok(zmq_drv_t *drv)
{
    zmqdrv_ok(drv, driver_caller(drv->port));
}

static void
zmqdrv_ok_bool(zmq_drv_t *drv, ErlDrvTermData pid, bool val)
{
    ErlDrvTermData spec[] = {
        ERL_DRV_ATOM,  am_zok,
        ERL_DRV_ATOM, (val ? am_true : am_false),
        ERL_DRV_TUPLE, 2
    };
    driver_send_term(drv->port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

static void
zmqdrv_ok_int64(zmq_drv_t *drv, ErlDrvTermData pid, int64_t val)
{
    ErlDrvTermData spec[] = {
        ERL_DRV_ATOM,   am_zok,
        ERL_DRV_INT64,  TERM_DATA(&val),
        ERL_DRV_TUPLE,  2
    };
    driver_send_term(drv->port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

static void
zmqdrv_ok_binary(zmq_drv_t *drv, ErlDrvTermData pid, void *data, size_t size)
{
    /* Copy payload. */
    ErlDrvTermData spec[] =
      {ERL_DRV_ATOM,   am_zok,
       ERL_DRV_BUF2BINARY, (ErlDrvTermData)data, size,
       ERL_DRV_TUPLE, 2};

    driver_send_term(drv->port, pid, spec, sizeof(spec)/sizeof(spec[0]));
}

//-------------------------------------------------------------------
// Driver callbacks
//-------------------------------------------------------------------

int zmqdrv_driver_init(void)
{
    INIT_ATOM(zok);
    INIT_ATOM(error);
    INIT_ATOM(eagain);
    INIT_ATOM(zmq);
    INIT_ATOM(msg);
    INIT_ATOM(true);
    INIT_ATOM(false);
    return 0;
}

/* Driver Start, called on port open. */
static ErlDrvData
zmqdrv_start(ErlDrvPort port, char* cmd)
{
#ifdef ZMQDRV_DEBUG_TO_FILE
	char filename[128];
	sprintf(filename, "./zmq_drv.%d.log", getpid());
	debug_file = fopen(filename, "a+");
#endif
    zmqdrv_fprintf("[zmq_drv] driver started by pid %ld\r\n", driver_connected(port));
    return reinterpret_cast<ErlDrvData>(new zmq_drv_t(port));
}

/* Driver Stop, called on port close. */
static void
zmqdrv_stop(ErlDrvData handle)
{
    delete reinterpret_cast<zmq_drv_t*>(handle);
    zmqdrv_fprintf("[zmq_drv] driver stopped by pid\r\n");
}

static void
zmqdrv_ready_input(ErlDrvData handle, ErlDrvEvent event)
{
    zmq_drv_t *drv = (zmq_drv_t *)handle;

    zmqdrv_fprintf("[zmq_drv] --> input ready on descriptor %ld\r\n", (long)event);

    // Get 0MQ sockets managed by application thread's signaler
    // identified by "event" fd.
#ifdef __WIN32__
	// in win32 we are signalled with the wsa event, not the socket descriptor
    zmq_wsa_events_map_t::iterator it = drv->zmq_wsa_events.find((HANDLE)event);
    assert(it != drv->zmq_wsa_events.end());
#else
    zmq_fd_sockets_map_t::iterator it = drv->zmq_fd_sockets.find((long)event);
    assert(it != drv->zmq_fd_sockets.end());
#endif

    zmq_sock_set_t::iterator si = it->second.begin();

    assert(si != it->second.end());

    for (; si != it->second.end(); ++si) {
        zmq_socket_t   s     = (*si)->socket;
		int			   fd    = (*si)->fd;
        uint32_t       idx   = (*si)->idx;
        ErlDrvTermData owner = (*si)->owner;
        int            rc    = 0;
        uint32_t       events;
        size_t         events_size = sizeof(events);

		// unregister event with erlang vm while we work with the socket
#ifdef __WIN32__
		driver_select(drv->port, (ErlDrvEvent) (*si)->wsa_event, ERL_DRV_READ, 0);
		zmqdrv_fprintf("[zmq_drv]		unregistered wsa event %ld on Erlang VM while we work on it\r\n", (long)(*si)->wsa_event);

		WSANETWORKEVENTS NetworkEvents;
		// necessary in order to reset the just fired event
		// http://msdn.microsoft.com/en-us/library/windows/desktop/ms741572(v=vs.85).aspx
		WSAEnumNetworkEvents(fd, (*si)->wsa_event, &NetworkEvents);
#else
		driver_select(drv->port, (ErlDrvEvent) fd, ERL_DRV_READ, 0);
		zmqdrv_fprintf("[zmq_drv]		unregistered socket %ld on Erlang VM while we work on it\r\n", (long)event);
#endif

		if (zmq_getsockopt(s, ZMQ_EVENTS, &events, &events_size) != 0) {
			zmqdrv_fprintf("[zmq_drv] zmq_getsockopt failed on socket %p(#%d)\r\n", s, idx);
			zmqdrv_error_code(drv, EINVAL);
			return;
		}

		zmqdrv_fprintf("[zmq_drv]		socket %p(#%d)\r\n", s, idx);
		zmqdrv_fprintf("[zmq_drv]		active: %s\r\n", (*si)->active_mode ? "yes" : "no");
		zmqdrv_fprintf("[zmq_drv]		in caller: %s(%p)\r\n", (*si)->in_caller != 0 ? "yes" : "no", (void*)(*si)->in_caller);
		zmqdrv_fprintf("[zmq_drv]		out caller: %s(%p)\r\n", (*si)->out_caller != 0 ? "yes" : "no", (void*)(*si)->out_caller);
		zmqdrv_fprintf("[zmq_drv]		zmq pollin: %s\r\n", events & ZMQ_POLLIN ? "yes" : "no");
		zmqdrv_fprintf("[zmq_drv]		zmq pollout: %s\r\n", events & ZMQ_POLLOUT ? "yes" : "no");

        while (((*si)->active_mode || (*si)->in_caller) && (events & ZMQ_POLLIN)) {
            msg_t msg;

			zmqdrv_fprintf("[zmq_drv] going to read zmq message on socket %p(#%d)...", s, idx);
            rc = zmq_recv(s, &msg, ZMQ_NOBLOCK);

            ErlDrvTermData pid = (*si)->active_mode ? owner : (*si)->in_caller;

            if (rc == -1) {
				zmqdrv_fprintf(" read failed...\r\n");
                if (zmq_errno() != EAGAIN) {
                    ErlDrvTermData spec[] =
                        {ERL_DRV_ATOM,  am_zmq,
                         ERL_DRV_UINT,  idx,
                         ERL_DRV_ATOM,  error_atom(zmq_errno()),
                         ERL_DRV_TUPLE, 2,
                         ERL_DRV_TUPLE, 3};
                    driver_send_term(drv->port, owner, spec, sizeof(spec)/sizeof(spec[0]));
                    (*si)->in_caller = 0;
                }
                break;
            }
			zmqdrv_fprintf(" successful read\r\n");

            if ((*si)->active_mode) {
                // Send message {zmq, Socket, binary()} to the owner pid
                ErlDrvTermData spec[] =
                    {ERL_DRV_ATOM,  am_zmq,
                     ERL_DRV_UINT,  idx,
                     ERL_DRV_BUF2BINARY, (ErlDrvTermData)zmq_msg_data(&msg), zmq_msg_size(&msg),
                     ERL_DRV_TUPLE, 3};
                driver_send_term(drv->port, owner, spec, sizeof(spec)/sizeof(spec[0]));
            } else {
                // Return result {ok, binary()} to the waiting caller's pid
                ErlDrvTermData spec[] = 
                    {ERL_DRV_ATOM,   am_zok,
                     ERL_DRV_BUF2BINARY, (ErlDrvTermData)zmq_msg_data(&msg), zmq_msg_size(&msg),
                     ERL_DRV_TUPLE, 2};
                driver_send_term(drv->port, pid, spec, sizeof(spec)/sizeof(spec[0]));
                (*si)->in_caller = 0;
            }

            // FIXME: add error handling
            zmqdrv_fprintf("[zmq_drv] received %d byte message relayed to pid %lu\r\n", zmq_msg_size(&msg), pid);
			if (zmq_getsockopt(s, ZMQ_EVENTS, &events, &events_size) != 0) {
				zmqdrv_fprintf("[zmq_drv] zmq_getsockopt failed on socket %p(#%d)\r\n", s, idx);
				zmqdrv_error_code(drv, EINVAL);
				return;
			}
        }
    
		if (zmq_getsockopt(s, ZMQ_EVENTS, &events, &events_size) != 0) {
			zmqdrv_fprintf("[zmq_drv] zmq_getsockopt failed on socket %p(#%d)\r\n", s, idx);
			zmqdrv_error_code(drv, EINVAL);
			return;
		}

        if ((*si)->out_caller != 0 && (events & ZMQ_POLLOUT)) {
            // There was a pending unwritten message on this socket.
            // Try to write it.  If the write succeeds/fails clear the ZMQ_POLLOUT
            // flag and notify the waiting caller of completion of operation.
            rc = zmq_send(s, &(*si)->out_msg, (*si)->out_flags | ZMQ_NOBLOCK);

			zmqdrv_fprintf("[zmq_drv] resending message %p (size %d) on socket %p (ret: %d)\r\n", 
                zmq_msg_data(&(*si)->out_msg), zmq_msg_size(&(*si)->out_msg), s, rc);

            if (rc == 0) {
                zmq_msg_close(&(*si)->out_msg);
                // Unblock the waiting caller's pid by returning result
                zmqdrv_ok(drv, (*si)->out_caller);
                (*si)->out_caller = 0;
            } else if (zmq_errno() != EAGAIN) {
                // Unblock the waiting caller's pid by returning result
                zmq_msg_close(&(*si)->out_msg);
                zmqdrv_socket_error(drv, (*si)->out_caller, idx, zmq_errno());
                (*si)->out_caller = 0;
            }
        }

		if (events == 0) {
			zmqdrv_fprintf("[zmq_drv]		spurious event triggered on socket %p(%d)\r\n", s, idx);
		}

		// reregister event with erlang vm if any pending operations exist
#ifdef __WIN32__
		driver_select(drv->port, (ErlDrvEvent) (*si)->wsa_event, ERL_DRV_READ, 1);
		zmqdrv_fprintf("[zmq_drv]		re-registered wsa event %ld on Erlang VM\r\n", (long)(*si)->wsa_event);
#else
		driver_select(drv->port, (ErlDrvEvent) fd, ERL_DRV_READ, 1);
		zmqdrv_fprintf("[zmq_drv]		re-registered socket %ld on Erlang VM\r\n", (long)event);
#endif
    }
}

// Called when an Erlang process owning sockets died.
// Perform cleanup of orphan sockets owned by pid.
static void 
zmqdrv_process_exit(ErlDrvData handle, ErlDrvMonitor* monitor)
{
    zmq_drv_t*     drv = (zmq_drv_t *)handle;
    ErlDrvTermData pid = driver_get_monitored_process(drv->port, monitor);

    zmqdrv_fprintf("[zmq_drv] detected death of process with pid %lu\r\n", pid);

    driver_demonitor_process(drv->port, monitor);

    // Walk through the list of sockets and close the ones
    // owned by pid.
    zmq_pid_sockets_map_t::iterator it = drv->zmq_pid_sockets.find(pid);

    if (it != drv->zmq_pid_sockets.end()) {
        zmqdrv_fprintf("[zmq_drv] pid %lu has %d sockets to be closed\r\n", pid, it->second.sockets.size());
        for(zmq_sock_set_t::iterator sit = it->second.sockets.begin();
            sit != it->second.sockets.end(); ++sit)
		{
			// request that del_socket not remove the element from the list as we are currently iterating that same list
            drv->del_socket((*sit)->idx, false);
		}
		// now that the iteration is finished, clear the whole list
		it->second.sockets.clear();
		zmqdrv_fprintf("[zmq_drv] removed all sockets owned by process pid %lu\r\n", pid);
    }
}

/* Erlang command, called on binary input from VM. */
static void
zmqdrv_outputv(ErlDrvData handle, ErlIOVec *ev)
{
    zmq_drv_t*    drv  = (zmq_drv_t *)handle;
    ErlDrvBinary* data = ev->binv[1];
    unsigned char cmd  = data->orig_bytes[0]; // First byte is the command

    zmqdrv_fprintf("[zmq_drv] driver got command %s on thread %p\r\n", debug_driver_command((driver_commands) cmd), erl_drv_thread_self());

    switch (cmd) {
        case ZMQ_INIT :
            zmqdrv_init(drv, ev);
            break;
        case ZMQ_TERM :
            zmqdrv_term(drv, ev);
            break;
        case ZMQ_SOCKET :
            zmqdrv_socket(drv, ev);
            break;
        case ZMQ_CLOSE :
            zmqdrv_close(drv, ev);
            break;
        case ZMQ_SETSOCKOPT :
            zmqdrv_setsockopt(drv, ev);
            break;
        case ZMQ_GETSOCKOPT :
            zmqdrv_getsockopt(drv, ev);
            break;
        case ZMQ_BIND :
            zmqdrv_bind(drv, ev);
            break;
        case ZMQ_CONNECT :
            zmqdrv_connect(drv, ev);
            break;
        case ZMQ_SEND :
            zmqdrv_send(drv, ev);
            break;
        case ZMQ_RECV :
            zmqdrv_recv(drv, ev);
            break;
        default :
            zmqdrv_error(drv, "Invalid driver command");
    }
}

static void
zmqdrv_init(zmq_drv_t *drv, ErlIOVec *ev)
{
    /* 
     * FIXME 
     * Use ei_decode_* to decode input from erlang VM.
     * This stuff is not documented anywhere, for now 
     * binary ErlIOVec is decoded by poking in iov struct.
     * 
     * Serge: Dhammika, ei_decode can only be used to decode
     * external binary format in the "output" callback function.
     * It's not suitable for using inside "outputv" body that
     * operates on I/O vectors unless you use term_to_binary/1
     * call to explicitely convert a term to external binary format.
     */

    uint32_t io_threads; 

    ErlDrvBinary* input = ev->binv[1];
    char* bytes = input->orig_bytes;
    io_threads  = ntohl(*(uint32_t *)(bytes + 1));

	zmqdrv_fprintf("[zmq_drv] # iothreads: %u\r\n", io_threads);

    if (drv->zmq_context) {
        zmqdrv_error_code(drv, EBUSY);
        return;
    }
    
    drv->zmq_context = (void *)zmq_init(io_threads);

    if (!drv->zmq_context) {
        zmqdrv_error_code(drv, zmq_errno());
        return;
    }

    zmqdrv_ok(drv);
}

static void
zmqdrv_term(zmq_drv_t *drv, ErlIOVec *ev)
{
    if (!drv->zmq_context) {
        zmqdrv_error_code(drv, ENODEV);
        return;
    }

    zmqdrv_fprintf("[zmq_drv] calling zmq_term(context) ...\r\n");
    int rc = zmq_term(drv->zmq_context);
    zmqdrv_fprintf("[zmq_drv] terminated zmq context\r\n");

    if (rc < 0) {
        zmqdrv_error_code(drv, zmq_errno());
        return;
    }

    zmqdrv_ok(drv);
    drv->zmq_context = NULL;
}

static void
zmqdrv_socket(zmq_drv_t *drv, ErlIOVec *ev)
{
    ErlDrvBinary* bin   = ev->binv[1];
    char*         bytes = bin->orig_bytes;
    int           type  = *(bytes + 1);

    void* s = zmq_socket(drv->zmq_context, type);
    if (!s) {
        zmqdrv_error_code(drv, zmq_errno());
        return;
    }

    int sig_fd;
    size_t sig_size = sizeof(sig_fd);
    zmq_getsockopt(s, ZMQ_FD, &sig_fd, &sig_size);

    if (sig_fd < 0) {
        zmqdrv_error(drv, "Invalid signaler");
        return;
    }

    // Register a new socket handle in order to avoid
    // passing actual address of socket to Erlang.  This
    // way it's more safe and also portable between 32 and
    // 64 bit OS's.
    uint32_t n = ++drv->zmq_socket_count;

    zmq_sock_info* zsi = new zmq_sock_info(s, n, driver_caller(drv->port), sig_fd);
    if (!zsi) {
        driver_failure_posix(drv->port, ENOMEM);
        return;
    }

#ifdef __WIN32__
	// create a new win32 event that we will associate with the newly created socket
	zsi->wsa_event = WSACreateEvent();
	// perform the association
	WSAEventSelect(zsi->fd, zsi->wsa_event, FD_READ | FD_WRITE);

	zmqdrv_fprintf("[zmq_drv] associated socket descriptor %d with wsa event %ld\r\n", zsi->fd, (long)zsi->wsa_event);
#endif

	zmqdrv_fprintf("[zmq_drv] created zmq socket %p(#%d), socket descriptor: %d, owner: %ld\r\n", s, n, zsi->fd, zsi->owner);

    drv->add_socket(zsi);
#ifdef __WIN32__
	driver_select(drv->port, (ErlDrvEvent)zsi->wsa_event, ERL_DRV_READ, 1);
	zmqdrv_fprintf("[zmq_drv] registered wsa event %ld on Erlang VM\r\n", (long)zsi->wsa_event);
#else
	driver_select(drv->port, (ErlDrvEvent)sig_fd, ERL_DRV_READ, 1);
	zmqdrv_fprintf("[zmq_drv] registered socket %d on Erlang VM\r\n", sig_fd);
#endif

    ErlDrvTermData spec[] = {ERL_DRV_ATOM,  am_zok,
                             ERL_DRV_UINT,  n,
                             ERL_DRV_TUPLE, 2};
    driver_send_term(drv->port, zsi->owner, spec, sizeof(spec)/sizeof(spec[0]));
}

static void
zmqdrv_close(zmq_drv_t *drv, ErlIOVec *ev)
{
    ErlDrvBinary* bin   = ev->binv[1];
    char*         bytes = bin->orig_bytes;
    uint32_t      idx   = ntohl(*(uint32_t*)(bytes+1));

    if (idx > drv->zmq_socket_count) {
        zmqdrv_error_code(drv, ENODEV);
        return;
    }

    int ret = drv->del_socket(idx, true);

	zmqdrv_fprintf("[zmq_drv] close #%d, result: %d\r\n", idx, ret);

    if (ret < 0) {
        zmqdrv_error_code(drv, zmq_errno());
        return;
    }
    
    zmqdrv_ok(drv);
}

static void 
zmqdrv_setsockopt(zmq_drv_t *drv, ErlIOVec *ev)
{
    ErlDrvBinary*  bin   = ev->binv[1];
    char*          bytes = bin->orig_bytes;
    uint32_t       idx   = ntohl(*(uint32_t*)(bytes+1));
    zmq_sock_info* si    = drv->get_socket_info(idx);
    uint8_t        n     = *(uint8_t*)(bytes+sizeof(idx)+1);
    char*          p     = bytes + 1 + sizeof(idx) + 1;

    if (idx > drv->zmq_socket_count || !si) {
        zmqdrv_error_code(drv, ENODEV);
        return;
    }

	zmqdrv_fprintf("[zmq_drv] setsockopt %p(#%d) (setting %d options)\r\n", si->socket, idx, (int)n);

    for (uint8_t j=0; j < n; ++j) {
        unsigned char option = *p++;
        uint64_t optvallen   = *p++;
        void*    optval      = p;

        switch (option) {
            case ZMQ_HWM:           assert(optvallen == 8);  break;
            case ZMQ_SWAP:          assert(optvallen == 8);  break;
            case ZMQ_AFFINITY:      assert(optvallen == 8);  break;
            case ZMQ_IDENTITY:      assert(optvallen < 256); break;
            case ZMQ_SUBSCRIBE:     assert(optvallen < 256); break;
            case ZMQ_UNSUBSCRIBE:   assert(optvallen < 256); break;
            case ZMQ_RATE:          assert(optvallen == 8);  break;
            case ZMQ_RECOVERY_IVL:  assert(optvallen == 8);  break;
            case ZMQ_MCAST_LOOP:    assert(optvallen == 8);  break;
            case ZMQ_SNDBUF:        assert(optvallen == 8);  break;
            case ZMQ_RCVBUF:        assert(optvallen == 8);  break;
            case ZMQ_ACTIVE:        assert(optvallen == 1);  break;
			default:				break;
        }

		zmqdrv_fprintf("[zmq_drv] setsockopt %p(#%d) (%s)\r\n", si->socket, idx, debug_zmq_socket_option((int)option));

        if (option == ZMQ_ACTIVE)
		{
            si->active_mode = *(char*)optval;
			zmqdrv_fprintf("[zmq_drv] %s socket %p(#%d)\r\n", si->active_mode ? "activating" : "deactivating", si->socket, idx);
		}
        else if (zmq_setsockopt(si->socket, option, optval, optvallen) < 0) {
            zmqdrv_error_code(drv, zmq_errno());
            return;
        }

        p += optvallen;
    }

    zmqdrv_ok(drv);
}

static void 
zmqdrv_getsockopt(zmq_drv_t *drv, ErlIOVec *ev)
{
    ErlDrvBinary*   bin   = ev->binv[1];
    char*           bytes = bin->orig_bytes;
    uint32_t        idx   = ntohl(*(uint32_t*)(bytes+1));
    void*           s     = drv->get_zmq_socket(idx);
    zmq_sock_info*  si    = drv->get_socket_info(idx);
    uint32_t        opt   = ntohl (*(uint32_t*)(bytes+sizeof(idx)+1));
    union {
        uint8_t  a[255];
        uint64_t ui64;
        int64_t  i64;
        int      i;
        uint32_t ui;
    } val;
    size_t vallen;

    if (idx > drv->zmq_socket_count || !s || !si) {
        zmqdrv_error_code(drv, ENODEV);
        return;
    }

	zmqdrv_fprintf("[zmq_drv] getsockopt socket %p(#%d), option: %d\r\n", si->socket, idx, opt);

    switch (opt) {
        case ZMQ_AFFINITY:
            vallen = sizeof(uint64_t);
            if (zmq_getsockopt(s, opt, &val.ui64, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.ui64);
            break;
        case ZMQ_BACKLOG:
            vallen = sizeof(int);
            if (zmq_getsockopt(s, opt, &val.i, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.i);
            break;
        case ZMQ_EVENTS:
            vallen = sizeof(uint32_t);
            if (zmq_getsockopt(s, opt, &val.ui, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.ui);
            break;
        case ZMQ_FD:
            vallen = sizeof(int);
            if (zmq_getsockopt(s, opt, &val.i, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.i);
            break;
        case ZMQ_HWM:
            vallen = sizeof(uint64_t);
            if (zmq_getsockopt(s, opt, &val.ui64, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.ui64);
            break;
        case ZMQ_IDENTITY:
            vallen = sizeof(val);
            if (zmq_getsockopt(s, opt, val.a, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_binary(drv, driver_caller(drv->port), val.a, vallen);
            break;
        case ZMQ_LINGER:
            vallen = sizeof(int);
            if (zmq_getsockopt(s, opt, &val.i, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_bool(drv, driver_caller(drv->port), !!val.i);
            break;
        case ZMQ_MCAST_LOOP:
            vallen = sizeof(int64_t);
            if (zmq_getsockopt(s, opt, &val.i64, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_bool(drv, driver_caller(drv->port), !!val.i64);
            break;
        case ZMQ_RATE:
            vallen = sizeof(int64_t);
            if (zmq_getsockopt(s, opt, &val.i64, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.i64);
            break;
        case ZMQ_RCVBUF:
            vallen = sizeof(uint64_t);
            if (zmq_getsockopt(s, opt, &val.ui64, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.ui64);
            break;
        case ZMQ_RCVMORE:
            vallen = sizeof(int64_t);
            if (zmq_getsockopt(s, opt, &val.i64, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_bool(drv, driver_caller(drv->port), !!val.i64);
            break;
        case ZMQ_RECONNECT_IVL:
            vallen = sizeof(int);
            if (zmq_getsockopt(s, opt, &val.i, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.i);
            break;
        case ZMQ_RECOVERY_IVL:
            vallen = sizeof(int64_t);
            if (zmq_getsockopt(s, opt, &val.i64, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.i64);
            break;
        case ZMQ_RECOVERY_IVL_MSEC:
            vallen = sizeof(int64_t);
            if (zmq_getsockopt(s, opt, &val.i64, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.i64);
            break;
        case ZMQ_SNDBUF:
            vallen = sizeof(uint64_t);
            if (zmq_getsockopt(s, opt, &val.ui64, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.ui64);
            break;
        case ZMQ_SWAP:
            vallen = sizeof(int64_t);
            if (zmq_getsockopt(s, opt, &val.i64, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.i64);
            break;
        case ZMQ_TYPE:
            vallen = sizeof(int);
            if (zmq_getsockopt(s, opt, &val.i, &vallen) < 0)
                zmqdrv_error_code(drv, zmq_errno());
            zmqdrv_ok_int64(drv, driver_caller(drv->port), val.i);
            break;
        case ZMQ_ACTIVE:
            zmqdrv_ok_bool(drv, driver_caller(drv->port), si->active_mode);
            break;
        default:
            zmqdrv_error(drv, "Option not implemented!");
            return;
    }
}

static void
zmqdrv_bind(zmq_drv_t *drv, ErlIOVec *ev)
{
    ErlDrvBinary* bin   = ev->binv[1];
    char*         bytes = bin->orig_bytes;
    uint16_t      size  = bin->orig_size - 5;
    uint32_t      idx   = ntohl(*(uint32_t*)(bytes+1));
    void*         s     = drv->get_zmq_socket(idx);
    char          addr[512];

    if (size > sizeof(addr) - 1) {
        zmqdrv_error_code(drv, E2BIG);
        return;
    }

    memcpy(addr, bytes + 5, size);
    addr[size] = '\0';

    if (idx > drv->zmq_socket_count || !s) {
        zmqdrv_error_code(drv, ENODEV);
        return;
    } else if (addr[0] == '\0') {
        zmqdrv_error_code(drv, EINVAL);
        return;
    }

    if (zmq_bind(s, addr) < 0) {
        zmqdrv_error_code(drv, zmq_errno());
        return;
    }

    int sig_fd;
    size_t sig_size = sizeof(sig_fd);
    zmq_getsockopt(s, ZMQ_FD, &sig_fd, &sig_size);	

	zmqdrv_fprintf("[zmq_drv] successful bind of zmq socket %p(#%d), socket descriptor %d to endpoint %s\r\n", s, idx, sig_fd, addr);

    zmqdrv_ok(drv);
}

static void
zmqdrv_connect(zmq_drv_t *drv, ErlIOVec *ev)
{
    ErlDrvBinary* bin   = ev->binv[1];
    char*         bytes = bin->orig_bytes;
    uint32_t      idx   = ntohl(*(uint32_t*)(bytes+1));
    void*         s     = drv->get_zmq_socket(idx);
    uint16_t      size  = bin->orig_size - 5;
    char          addr[512];

    if (idx > drv->zmq_socket_count || !s) {
        zmqdrv_error_code(drv, ENODEV);
        return;
    }

    if (size > sizeof(addr) - 1) {
        zmqdrv_error_code(drv, E2BIG);
        return;
    }

    memcpy(addr, bytes + 5, size);
    addr[size] = '\0';

    if (!addr[0]) {
        zmqdrv_error_code(drv, EINVAL);
        return;
    }

    if (zmq_connect(s, addr) < 0) {
        zmqdrv_error_code(drv, zmq_errno());
        return;
    }

	zmqdrv_fprintf("[zmq_drv] socket %p(#%d) connected to %s\r\n", s, idx, addr);

    zmqdrv_ok(drv);
}

static void
zmqdrv_send(zmq_drv_t *drv, ErlIOVec *ev)
{
    ErlDrvBinary*  bin   = ev->binv[1];
    char*          bytes = bin->orig_bytes;
    uint32_t       idx   = ntohl(*(uint32_t*)(bytes+1));
    zmq_sock_info* si    = drv->get_socket_info(idx);
    uint32_t       flags = ntohl(*(uint32_t*)(bytes+5));
    void*          data  = (void *)(bytes + 9);
    size_t         size  = bin->orig_size - 9;

    if (idx > drv->zmq_socket_count || !si) {
        zmqdrv_error_code(drv, ENODEV);
        return;
    }

    if (si->out_caller != 0) {
        // There's still an unwritten message pending
		zmqdrv_fprintf("[zmq_drv] there's still an unwritten message pending on socket %p(#%d)\r\n", 
			si->socket, idx);
        zmqdrv_error_code(drv, EBUSY);
        return;
    }

#ifdef ZMQDRV_DEBUG
    uint32_t events;
    size_t events_size = sizeof(events);
    zmq_getsockopt(si->socket, ZMQ_EVENTS, &events, &events_size);
	zmqdrv_fprintf("[zmq_drv] sending %u bytes of data on zmq socket %p(#%d) (events: %d)\r\n", 
			size, si->socket, idx, events);
#endif

    // Increment the reference count on binary so that zmq can
    // take ownership of it.
    driver_binary_inc_refc(bin);

    if (zmq_msg_init_data(&si->out_msg, data, size, &zmq_free_binary, bin)) {
        zmqdrv_error_code(drv, zmq_errno());
        driver_binary_dec_refc(bin);
        return;
    }

    if (zmq_send(si->socket, &si->out_msg, flags | ZMQ_NOBLOCK) == 0) {
        zmqdrv_ok(drv);
#ifdef __WIN32__
		zmqdrv_ready_input((ErlDrvData)drv, (ErlDrvEvent)si->wsa_event);
#else
        zmqdrv_ready_input((ErlDrvData)drv, (ErlDrvEvent)si->fd);
#endif
    } else {
        int e = zmq_errno();
        if (e == EAGAIN) {
            // No msg returned to caller - make him wait until async
            // send succeeds
            si->out_caller = driver_caller(drv->port);
            return;
        }
        zmqdrv_error_code(drv, e);
    }
    zmq_msg_close(&si->out_msg);
}

static void
zmqdrv_recv(zmq_drv_t *drv, ErlIOVec *ev)
{
    ErlDrvBinary*  bin   = ev->binv[1];
    char*          bytes = bin->orig_bytes;
    uint32_t       idx   = ntohl(*(uint32_t*)(bytes+1));
    zmq_sock_info* si    = drv->get_socket_info(idx);

    if (idx > drv->zmq_socket_count || !si) {
        zmqdrv_error_code(drv, ENODEV);
        return;
    }

    if (si->active_mode) {
        zmqdrv_error_code(drv, EINVAL);
        return;
    }

    if (si->in_caller != 0) {
        // Previous recv() call in passive mode didn't complete.
        // The owner must be blocked waiting for result.
		zmqdrv_fprintf("[zmq_drv] previous recv call in passive mode didn't complete on socket %p(#%d)\r\n", si->socket, idx);
        zmqdrv_error_code(drv, EBUSY);
        return;
    }

    uint32_t events;
    size_t events_size = sizeof(events);
	if (zmq_getsockopt(si->socket, ZMQ_EVENTS, &events, &events_size) != 0) {
		zmqdrv_fprintf("[zmq_drv] zmq_getsockopt failed on socket %p(#%d)\r\n", si->socket, idx);
        zmqdrv_error_code(drv, EINVAL);
        return;
	}

    if (events == 0) {
        si->in_caller = driver_caller(drv->port);
		zmqdrv_fprintf("[zmq_drv] no data ready for socket %p(#%d), blocking caller: %lu\r\n", si->socket, idx, si->in_caller);
	}
    else {
        msg_t msg;

        if (zmq_recv(si->socket, &msg, ZMQ_NOBLOCK) == 0)
            zmqdrv_ok_binary(drv, driver_caller(drv->port), zmq_msg_data(&msg), zmq_msg_size(&msg));
        else if (zmq_errno() == EAGAIN) {
			zmqdrv_fprintf("[zmq_drv] no input available on socket %p(#%d)\r\n", si->socket, idx);
            // No input available. Make the caller wait by not returning result
            si->in_caller = driver_caller(drv->port);
        } else
		{
			zmqdrv_fprintf("[zmq_drv] error reading data on socket %p(#%d): %p\r\n", si->socket, idx, zmq_strerror(zmq_errno()));
            zmqdrv_error_code(drv, zmq_errno());
		}
    }
}
