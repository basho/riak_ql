-module(parser_describe_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

simple_describe_test() ->
    ?sql_comp_assert("describe GeoCheckins",
                     #riak_sql_describe_v1{'DESCRIBE' = <<"GeoCheckins">>}).

uppercase_quoted_describe_test() ->
    ?sql_comp_assert("DESCRIBE \"GeoCheckins\"",
                     #riak_sql_describe_v1{'DESCRIBE' = <<"GeoCheckins">>}).
