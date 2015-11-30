PROJECT = riak_ql

DEPS = edown sext

TEST_DEPS = unite

dep_unite = git https://github.com/eproxus/unite.git v0.0.1
dep_sext = git git://github.com/basho/sext 1.1p3
dep_edown = git git://github.com/uwiger/sext 0.5

EUNIT_OPTS = no_tty, {report, {unite_compact, []}}

SHELL_OPTS = -sname riak_ql -setcookie riak_ql

include erlang.mk
