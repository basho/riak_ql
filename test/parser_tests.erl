-module(parser_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("riak_ql_ddl.hrl").

not_null_white_space_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time TIMESTAMP NOT                NULL, "
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
        {ok, #ddl_v1{}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
    ).

create_table_white_space_test() ->
    Table_def =
        "CREATE               \tTABLE temperatures ("
        "time TIMESTAMP NOT NULL, "
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
        {ok, #ddl_v1{}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
    ).

primary_key_white_space_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time TIMESTAMP NOT NULL, "
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY               \t  KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
        {ok, #ddl_v1{}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
    ).

missing_primary_key_error_test() ->
    ?assertEqual(
        {error,{'$undefined',riak_ql_parser,"primary_key_missing"}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens("CREATE TABLE bad8 ( f sint64 )"))
    ).

missing_primary_key_no_fields_error_test() ->
    ?assertMatch(
        {error, _}, % syntax error, just make sure that ti fails
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(
            "CREATE TABLE bad8 ( )"))
    ).
