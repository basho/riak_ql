-module(parser_select_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

select_sql_test() ->
    ?sql_comp_assert("select * from argle",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"argle">>}).

select_sql_with_semicolon_test() ->
    ?sql_comp_assert("select * from argle;",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"argle">>}).

select_sql_with_semicolons_in_quotes_test() ->
    ?sql_comp_assert("select * from \"table;name\" where ';' = asdf;",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"table;name">>,
                                 'WHERE'  = [{'=', <<"asdf">>, {binary, <<";">>}}]
                                }).

select_sql_semicolon_second_statement_test() ->
    ?sql_comp_fail("select * from asdf; select * from asdf").

select_sql_multiple_semicolon_test() ->
    ?sql_comp_fail("select * from asdf;;").

select_quoted_sql_test() ->
    ?sql_comp_assert("select * from \"argle\"",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"argle">>}).

select_quoted_keyword_sql_test() ->
    ?sql_comp_assert("select * from \"select\"",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"select">>}).

select_nested_quotes_sql_test() ->
    ?sql_comp_assert("select * from \"some \"\"quotes\"\" in me\"",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"some \"quotes\" in me">>}).

select_from_lists_sql_test() ->
    ?sql_comp_assert("select * from events, errors",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = {list, [<<"events">>, <<"errors">>]}
                                }).

select_fields_from_lists_sql_test() ->
    ?sql_comp_assert("select hip, hop, dont, stop from events",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                                                          {identifier, [<<"hip">>]},
                                                                          {identifier, [<<"hop">>]},
                                                                          {identifier, [<<"dont">>]},
                                                                          {identifier, [<<"stop">>]}
                                                                         ]},
                                 'FROM'   = <<"events">>
                                }).
