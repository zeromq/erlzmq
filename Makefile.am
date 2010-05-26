TARBALL=zmq.tgz
TARBALL_EXCLUDE=/tmp/exclude.xxx

SUBDIRS = src

#docs:
#	@${MAKE} --directory=src $@

gitdocs: docs
	./bin/update_docs

tar:
	@echo Creating $(TARBALL)
	@DIR=$${PWD##*/} && pushd .. > /dev/null && \
	echo -e "*.o\n*.d\n.git\n*.tgz\n.*.*\n~\n.~*#\ntags\n" > $(TARBALL_EXCLUDE) && \
	for f in priv; do \
        find $$DIR/$$f -type f -print >> $(TARBALL_EXCLUDE) ; \
	done && \
	tar zcf $(TARBALL) $$DIR --exclude-from $(TARBALL_EXCLUDE) && \
	mv $(TARBALL) $$DIR && \
	popd > /dev/null && \
	rm -f $(TARBALL_EXCLUDE) && \
	ls -l $(TARBALL)

