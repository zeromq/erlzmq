ZMQ_DIR=/opt/zeromq-2.0.6

ERL ?= erl
ERL_INTERFACE_DIR=$(shell \
	$(ERL) -eval 'io:format("~s\n", [code:lib_dir(erl_interface)]).' \
	       -noshell -s erlang halt)
ERL_INSTALL_DIR := $(patsubst %/,%,$(dir $(ERL_INTERFACE_DIR)))
ERL_INSTALL_DIR := $(patsubst %/,%,$(dir $(ERL_INSTALL_DIR)))

ifeq (,$(ERL_INSTALL_DIR))
$(error Erlang installation directory not found)
endif

LDFLAGS=-shared -fPIC ${ERL_INTERFACE_DIR}/lib/libei.a -L${ZMQ_DIR}/lib -lzmq -lpthread -luuid
CFLAGS=-Wall -ggdb -O0 -I${ZMQ_DIR}/include \
	   -I$(ERL_INTERFACE_DIR)/include \
	   -I$(ERL_INSTALL_DIR)/usr/include
CC=g++

ifdef debug
CFLAGS += -DZMQDRV_DEBUG
endif

all: ../priv/zmq_drv.so

../priv/zmq_drv.so: zmq_drv.cpp
	$(CC) -o $@ $< ${LDFLAGS} ${CFLAGS}

clean:
	rm -rf zmq_drv.o ../priv/zmq_drv.so
