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

test: compile
	./rebar eunit skip_deps=true
