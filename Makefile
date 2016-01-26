PROJECT = riak_ql

DEPS = edown sext mochiweb

TEST_DEPS = unite

dep_unite = git https://github.com/eproxus/unite.git v0.0.1
dep_sext = git https://github.com/basho/sext 1.1p3
dep_edown = git https://github.com/uwiger/edown 0.5
dep_mochiweb = git https://github.com/basho/mochiweb.git v2.9.0p1

EUNIT_OPTS = no_tty, {report, {unite_compact, []}}

SHELL_OPTS = -sname riak_ql -setcookie riak_ql

include erlang.mk
