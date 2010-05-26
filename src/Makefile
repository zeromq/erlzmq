ERL_SOURCES=$(wildcard *.erl)
EBIN_DIR=../ebin
DOC_DIR=../doc
ERL_FLAGS= +debug_info -I../include
ERL_OBJECTS=$(ERL_SOURCES:%.erl=$(EBIN_DIR)/%.beam)
#DOC_FILES=$(ERL_SOURCES:%.erl=$(DOC_DIR)/%.html)
DOC_FILES=$(DOC_DIR)/zmq.html

ifdef debug
ERL_FLAGS += -Ddebug
endif

include ../vsn.mk

all: $(ERL_OBJECTS) $(EBIN_DIR)/zmq.app

$(EBIN_DIR)/%.beam: %.erl
	erlc $(ERL_FLAGS) -o ../ebin $<

$(EBIN_DIR)/zmq.app: zmq.app.src ../vsn.mk
	sed 's/%VSN%/$(VSN)/g' $< > $@

clean:
	rm -f $(ERL_OBJECTS)
	rm -fr $(DOC_DIR)

docs: $(DOC_DIR) $(DOC_FILES)

$(DOC_DIR):
	mkdir $@

#$(DOC_DIR)/overview.html: overview.edoc
#	erl -noshell -run edoc_run file '"$<"' '[{dir, "$(DOC_DIR)"}]' -run init stop
$(DOC_DIR)/zmq.html: zmq.erl overview.edoc
	erl -noshell -run edoc_run application "'zmq'" '"."' '[{dir, "$(DOC_DIR)"}]' -run init stop

