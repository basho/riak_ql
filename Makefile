.PHONY: deps test

DIALYZER_FLAGS =

all: deps compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean
	rm -rf test.*-temp-data

distclean: clean
	./rebar delete-deps

test:
	@# delete the lexer and parser beams because the rebar2 will not
	@# recompile them on changes
	@rm -f .eunit/riak_ql_parser.beam .eunit/riak_ql_lexer.beam
	./rebar eunit skip_deps=true

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler

shell:
	@# the shell command is broken in the riak_ql version of rebar and doesn't
	@# do distribution anyway.
	erl -pa ebin -pa deps/*/ebin -sname riak_ql -setcookie riak_ql

include tools.mk
