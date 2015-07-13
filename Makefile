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

DIALYZER_APPS = kernel stdlib erts tools os_mon runtime_tools mnesia eunit \
		syntax_tools compiler

include tools.mk
