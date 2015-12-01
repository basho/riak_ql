-module(lexer_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("riak_ql_ddl.hrl").

symbols_in_identifier_1_test() ->
    ?assertError(
        <<"Unexpected token '^'.">>,
        riak_ql_lexer:get_tokens(
            "CREATE TABLE ^ ("
            "time TIMESTAMP NOT NULL, "
            "family VARCHAR NOT NULL, "
            "series VARCHAR NOT NULL, "
            "PRIMARY KEY "
            " ((family, series, quantum(time, 15, 's')), family, series, time))")
    ).

symbols_in_identifier_2_test() ->
    ?assertError(
        <<"Unexpected token '&'.">>,
        riak_ql_lexer:get_tokens("klsdafj kljfd (*((*& 89& 8KHH kJHkj hKJH K K")
    ).

symbols_in_identifier_3_test() ->
    ?assertError(
        <<"Unexpected token '$'.">>,
        riak_ql_lexer:get_tokens(
            "CREATE TABLE mytable ("
            "time TIMESTAMP NOT NULL, "
            "family $ NOT NULL, "
            "series VARCHAR NOT NULL, "
            "PRIMARY KEY "
            " ((family, series, quantum(time, 15, 's')), family, series, time))")
    ).

symbols_in_identifier_4_test() ->
    ?assertError(
        <<"Unexpected token ']'.">>,
        riak_ql_lexer:get_tokens("select ] from a")
    ).