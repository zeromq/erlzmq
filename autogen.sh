#!/bin/sh
# Run this to generate all the initial makefiles, etc.

aclocal  
libtoolize --automake
automake -a
autoconf

