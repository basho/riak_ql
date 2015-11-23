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