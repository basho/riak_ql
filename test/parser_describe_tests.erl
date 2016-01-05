-module(parser_describe_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("riak_ql_ddl.hrl").

-define(sql_comp_assert(String, Expected),
        Toks = riak_ql_lexer:get_tokens(String),
        Got = riak_ql_parser:parse(Toks),
%% io:format(standard_error, "~p~n~p~n", [Exp2, Got]),
        ?assertEqual({ok, Expected}, Got)).

simple_describe_test() ->
    ?sql_comp_assert("describe GeoCheckins",
                     #riak_sql_describe_v1{'DESCRIBE' = <<"GeoCheckins">>}).

uppercase_quoted_describe_test() ->
    ?sql_comp_assert("DESCRIBE \"GeoCheckins\"",
                     #riak_sql_describe_v1{'DESCRIBE' = <<"GeoCheckins">>}).
