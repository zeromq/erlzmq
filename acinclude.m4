# AC_ERLANG_SUBST_ERTS_VER
# -------------------------------------------------------------------
AC_DEFUN([AC_ERLANG_SUBST_ERTS_VER],
    [AC_REQUIRE([AC_ERLANG_NEED_ERLC])[]dnl
     AC_REQUIRE([AC_ERLANG_NEED_ERL])[]dnl
     AC_CACHE_CHECK([for Erlang/OTP ERTS version],
        [erlang_cv_erts_ver],
        [AC_LANG_PUSH(Erlang)[]dnl
        AC_RUN_IFELSE(
            [AC_LANG_PROGRAM([], [dnl
                Version = erlang:system_info(version),
                file:write_file("conftest.out", Version),
                halt(0)])],
            [erlang_cv_erts_ver=`cat conftest.out`],
            [AC_MSG_FAILURE([test Erlang program execution failed])])
        AC_LANG_POP(Erlang)[]dnl
        ])
    AC_SUBST([ERLANG_ERTS_VER], [$erlang_cv_erts_ver])
    ])# AC_ERLANG_SUBST_ERTS_VER
