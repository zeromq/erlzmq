REPO		?= erlzmq
ZMQ_TAG	 = $(shell git describe --tags --always)
REVISION	?= $(shell echo $(ZMQ_TAG) | sed -e 's/^$(REPO)-//')
PKG_VERSION	?= $(shell echo $(REVISION) | tr - .)

all:
	./rebar compile

docs: all
	./rebar doc	

gitdocs: docs
	/bin/update_docs
	

clean:
	@rm -f doc/*.html doc/*.css doc/*.png doc/edoc-info
	./rebar clean

# Release tarball creation
# Generates a tarball that includes all the deps sources so no checkouts are necessary

archive = git archive --format=tar --prefix=$(1)/ HEAD | (cd $(2) && tar xf -)

buildtar = mkdir distdir && \
		 git clone . distdir/$(REPO)-clone && \
		 cd distdir/$(REPO)-clone && \
		 git checkout $(ZMQ_TAG) && \
		 $(call archive,$(ZMQ_TAG),..) && \
		 mkdir ../$(ZMQ_TAG)/deps && \
		 make deps; \
		 for dep in deps/*; do cd $${dep} && $(call archive,$${dep},../../../$(ZMQ_TAG)); cd ../..; done
					 
distdir:
	$(if $(ZMQ_TAG), $(call buildtar), $(error "You can't generate a release tarball from a non-tagged revision. Run 'git checkout <tag>', then 'make dist'"))

dist $(ZMQ_TAG).tar.gz: distdir
	cd distdir; \
	tar czf ../$(ZMQ_TAG).tar.gz $(ZMQ_TAG)

distclean: ballclean
	rm -rf dist

ballclean:
	rm -rf $(ZMQ_TAG).tar.gz distdir

package: dist
	$(MAKE) -C package package

pkgclean:
	$(MAKE) -C package pkgclean

export ZMQ_TAG PKG_VERSION REPO REVISION

