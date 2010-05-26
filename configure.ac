dnl Process this file with autoconf to produce a configure script
AC_PREREQ(2.56)
AC_INIT(zmq)
AM_INIT_AUTOMAKE(appexp, 0.1.00)
AC_PROG_CC
AC_PROG_RANLIB

AC_ERLANG_CHECK_LIB([kernel])
AC_ERLANG_CHECK_LIB([stdlib])

AC_ERLANG_SUBST_ERTS_VER

AC_OUTPUT(c_src/Makefile src/Makefile Makefile)

