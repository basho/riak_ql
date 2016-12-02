%% -------------------------------------------------------------------
%%
%% SELECT command tests for the Parser
%%
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(parser_select_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

select_sql_test() ->
    ?sql_comp_assert_match(
       "select * from argle", select,
       [{fields, [
                  {identifier, [<<"*">>]}
                 ]},
        {tables, <<"argle">>},
        {where, []}
       ]).

select_sql_with_semicolon_test() ->
    ?sql_comp_assert_match(
       "select * from argle;", select,
       [{fields, [
                  {identifier, [<<"*">>]}
                 ]},
        {tables, <<"argle">>},
        {where, []}
       ]).

select_sql_with_semicolons_in_quotes_test() ->
    ?sql_comp_assert_match(
       "select * from \"table;name\" where ';' = asdf;", select,
       [{fields, [
                  {identifier, [<<"*">>]}
                 ]},
        {tables, <<"table;name">>},
        {where, [{'=', <<"asdf">>, {binary, <<";">>}}]}
       ]).

select_sql_semicolon_second_statement_test() ->
    ?sql_comp_fail("select * from asdf; select * from asdf").

select_sql_multiple_semicolon_test() ->
    ?sql_comp_fail("select * from asdf;;").

select_quoted_sql_test() ->
    ?sql_comp_assert_match(
       "select * from \"argle\"", select,
       [{fields, [
                  {identifier, [<<"*">>]}
                 ]},
        {tables, <<"argle">>},
        {where, []}
       ]).

select_quoted_keyword_sql_test() ->
    ?sql_comp_assert_match(
       "select * from \"select\"", select,
       [{fields, [
                  {identifier, [<<"*">>]}
                 ]},
        {tables, <<"select">>},
        {where, []}
       ]).

select_nested_quotes_sql_test() ->
    ?sql_comp_assert_match(
       "select * from \"some \"\"quotes\"\" in me\"", select,
       [{fields, [
                  {identifier, [<<"*">>]}
                 ]},
        {tables, <<"some \"quotes\" in me">>},
        {where, []}
       ]).

select_from_lists_sql_test() ->
    ?sql_comp_fail("select * from events, errors").

select_from_lists_with_where_sql_test() ->
    ?sql_comp_fail("select foo from events, errors where x = y").

select_fields_from_lists_sql_test() ->
    ?sql_comp_assert_match(
       "select hip, hop, dont, stop from events", select,
       [{fields, [
                  {identifier, [<<"hip">>]},
                  {identifier, [<<"hop">>]},
                  {identifier, [<<"dont">>]},
                  {identifier, [<<"stop">>]}
                 ]},
        {tables, <<"events">>},
        {where, []}
       ]).

select_quoted_spaces_sql_test() ->
    ?sql_comp_assert_match(
       "select * from \"table with spaces\"", select,
       [{fields, [
                  {identifier, [<<"*">>]}
                 ]},
        {tables, <<"table with spaces">>},
        {where, []}
       ]).

select_quoted_escape_sql_test() ->
    ?sql_comp_assert_match(
       "select * from \"table with spaces\" where "
       "\"co\"\"or\" = 'klingon''name' or "
       "\"co\"\"or\" = '\"'", select,
       [{fields, [
                  {identifier, [<<"*">>]}
                 ]},
        {tables, <<"table with spaces">>},
        {where, [
                 {or_,
                  {'=', <<"co\"or">>, {binary, <<"\"">>}},
                  {'=', <<"co\"or">>, {binary, <<"klingon'name">>}}
                 }
                ]}
       ]).

group_by_one_field_test() ->
    Query_sql =
        "SELECT b FROM mytab "
        "WHERE a = 1 "
        "GROUP BY b",
    ?assertEqual(
        {select, [
                  {tables, <<"mytab">>},
                  {fields, [{identifier, [<<"b">>]}]},
                  {where,  [{'=', <<"a">>, {integer, 1}}]},
                  {group_by, [{identifier, <<"b">>}]},
                  {limit, []},
                  {offset, []},
                  {order_by, []}
                 ]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

group_by_two_fields_test() ->
    Query_sql =
        "SELECT a, b FROM mytab "
        "WHERE a = 1 "
        "GROUP BY a, b",
    ?assertEqual(
        {select, [
                  {tables, <<"mytab">>},
                  {fields, [{identifier, [<<"a">>]}, {identifier, [<<"b">>]}]},
                  {where,  [{'=', <<"a">>, {integer, 1}}]},
                  {group_by, [{identifier, <<"a">>}, {identifier, <<"b">>}]},
                  {limit, []},
                  {offset, []},
                  {order_by, []}
                 ]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

order_by_1_test() ->
    Query_sql =
        "SELECT a, b FROM mytab "
        "WHERE a = 1 "
        "ORDER BY a, b",  %% different default null-specs depending on ordering-spec
    ?assertEqual(
        {select, [
                  {tables, <<"mytab">>},
                  {fields, [{identifier, [<<"a">>]}, {identifier, [<<"b">>]}]},
                  {where,  [{'=', <<"a">>, {integer, 1}}]},
                  {group_by, []},
                  {limit, []},
                  {offset, []},
                  {order_by, [{<<"a">>, asc, nulls_last}, {<<"b">>, asc, nulls_last}]}
                 ]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

order_by_2_test() ->
    Query_sql =
        "SELECT a, b FROM mytab "
        "WHERE a = 1 "
        "ORDER BY a desc, b asc",  %% different defaults for null-spec #2
    ?assertEqual(
        {select, [
                  {tables, <<"mytab">>},
                  {fields, [{identifier, [<<"a">>]}, {identifier, [<<"b">>]}]},
                  {where,  [{'=', <<"a">>, {integer, 1}}]},
                  {group_by, []},
                  {limit, []},
                  {offset, []},
                  {order_by, [{<<"a">>, desc, nulls_first}, {<<"b">>, asc, nulls_last}]}
                 ]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

order_by_3_test() ->
    Query_sql =
        "SELECT a, b FROM mytab "
        "WHERE a = 1 "
        "ORDER BY a, b LIMIT 11 OFFSET 22",
    ?assertEqual(
        {select, [
                  {tables, <<"mytab">>},
                  {fields, [{identifier, [<<"a">>]}, {identifier, [<<"b">>]}]},
                  {where,  [{'=', <<"a">>, {integer, 1}}]},
                  {group_by, []},
                  {limit, [11]},
                  {offset, [22]},
                  {order_by, [{<<"a">>, asc, nulls_last}, {<<"b">>, asc, nulls_last}]}
                 ]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

order_by_4_test() ->
    Query_sql =
        "SELECT a, b FROM mytab "
        "WHERE a = 1 "
        "LIMIT 11",  %% implied, empty ORDER BY
    ?assertEqual(
        {select, [
                  {tables, <<"mytab">>},
                  {fields, [{identifier, [<<"a">>]}, {identifier, [<<"b">>]}]},
                  {where,  [{'=', <<"a">>, {integer, 1}}]},
                  {group_by, []},
                  {limit, [11]},
                  {offset, []},
                  {order_by, []}
                 ]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

order_by_with_group_by_test() ->
    Query_sql =
        "SELECT a, b FROM mytab "
        "WHERE a = 1 "
        "GROUP BY a, b ORDER BY a",
    ?assertEqual(
       {error, {0, riak_ql_parser, <<"ORDER BY/LIMIT/OFFSET clauses are not supported for GROUP BY queries.">>}},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

selection_fields_must_be_in_group_by_1_test() ->
    Query_sql =
        "SELECT c FROM mytab "
        "WHERE a = 1 "
        "GROUP BY a, b",
    ?assertEqual(
        {error, {0, riak_ql_parser, <<"Field(s) c are specified in the select statement but not the GROUP BY.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

selection_fields_must_be_in_group_by_2_test() ->
    Query_sql =
        "SELECT c, d FROM mytab "
        "WHERE a = 1 "
        "GROUP BY a, b",
    ?assertEqual(
        {error, {0, riak_ql_parser, <<"Field(s) c, d are specified in the select statement but not the GROUP BY.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

selection_fields_must_be_in_group_by_3_test() ->
    Query_sql =
        "SELECT a, d FROM mytab "
        "WHERE a = 1 "
        "GROUP BY a, b",
    ?assertEqual(
        {error, {0, riak_ql_parser, <<"Field(s) d are specified in the select statement but not the GROUP BY.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

selection_fields_must_be_in_group_by_arithmetic_left_test() ->
    Query_sql =
        "SELECT d + 1 FROM mytab "
        "WHERE a = 1 "
        "GROUP BY a, b",
    ?assertEqual(
        {error, {0, riak_ql_parser, <<"Field(s) d are specified in the select statement but not the GROUP BY.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

selection_fields_must_be_in_group_by_arithmetic_left_right_test() ->
    Query_sql =
        "SELECT d + e FROM mytab "
        "WHERE a = 1 "
        "GROUP BY a, b",
    ?assertEqual(
        {error, {0, riak_ql_parser, <<"Field(s) d, e are specified in the select statement but not the GROUP BY.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

selection_fields_must_be_in_group_by_arithmetic_right_test() ->
    Query_sql =
        "SELECT 2 + d FROM mytab "
        "WHERE a = 1 "
        "GROUP BY a, b",
    ?assertEqual(
        {error, {0, riak_ql_parser, <<"Field(s) d are specified in the select statement but not the GROUP BY.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

select_all_not_allowed_as_column_with_group_by_test() ->
    Query_sql =
        "SELECT * FROM mytab "
        "WHERE a = 1 "
        "GROUP BY a, b",
    ?assertEqual(
        {error, {0, riak_ql_parser, <<"Field(s) * are specified in the select statement but not the GROUP BY.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

select_all_not_allowed_in_group_by_test() ->
    Query_sql =
        "SELECT AVG(x) FROM mytab "
        "WHERE a = 1 "
        "GROUP BY *, b",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"GROUP BY can only contain table columns but '*' was found.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

field_in_aggregate_function_does_not_have_to_be_in_group_by_test() ->
    Query_sql =
        "SELECT AVG(x) FROM mytab "
        "WHERE a = 1 "
        "GROUP BY a, b",
    ?assertMatch(
        {select, [_|_]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

select_hex_test() ->
    Query_sql =
        "SELECT * FROM mytab "
        "WHERE a = 0xDEADBEEF",
    {select, Parsed_query} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql)),
    ?assertEqual(
        {where, [{'=', <<"a">>, {binary, mochihex:to_bin("DEADBEEF")}}]},
        proplists:lookup(where, Parsed_query)
    ).

select_hex_all_chars_test() ->
    All_chars = "0123456789aBbBcCdDeEfF",
    Query_sql =
        "SELECT * FROM mytab "
        "WHERE a = 0x"++All_chars,
    {select, Parsed_query} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql)),
    ?assertEqual(
        {where, [{'=', <<"a">>, {binary, mochihex:to_bin(All_chars)}}]},
        proplists:lookup(where, Parsed_query)
    ).

select_empty_hex_test() ->
    Query_sql =
        "SELECT * FROM mytab "
        "WHERE a = 0x",
    {select, Parsed_query} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql)),
    ?assertEqual(
        {where, [{'=', <<"a">>, {binary, << >>}}]},
        proplists:lookup(where, Parsed_query)
    ).

select_empty_hex_not_as_final_token_test() ->
    Query_sql =
        "SELECT * FROM mytab "
        "WHERE a = 0x AND b = 10",
    {select, Parsed_query} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql)),
    ?assertEqual(
        {where, [{and_, {'=', <<"a">>, {binary, << >>}}, {'=',<<"b">>,{integer,10}}}]},
        proplists:lookup(where, Parsed_query)
    ).

select_empty_hex_pattern_in_single_quotes_test() ->
    Query_sql =
        "SELECT * FROM mytab "
        "WHERE a = '0xABABABAB'",
    {select, Parsed_query} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql)),
    ?assertEqual(
        {where, [{'=', <<"a">>, {binary, <<"0xABABABAB">>}}]},
        proplists:lookup(where, Parsed_query)
    ).

select_hex_odd_number_of_chars_in_hex_test() ->
    Query_sql =
        "SELECT * FROM mytab "
        "WHERE a = 0x0DDBEEF",
    ?assertException(
        error, {odd_hex_chars, <<"Hex strings must have an even number of characters.">>},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Query_sql))
    ).

select_hex_and_char_literals_parse_the_same_test() ->
    Text = "QWERTY",
    Varchar_sql =
      "SELECT * FROM mytab "
      "WHERE a = '"++Text++"'" ,
    Hex_sql =
      "SELECT * FROM mytab "
      "WHERE a = 0x"++mochihex:to_hex(Text),
    ?assertEqual(
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Varchar_sql)),
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Hex_sql))
    ).

multiline_comment_on_single_line_in_select_test() ->
    ?assertEqual(
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab "
            "WHERE a = 'val'")),
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
           "SELECT * FROM mytab /* hi */ "
           "WHERE a = 'val'"))
    ).

multiline_comment_in_select_test() ->
    ?assertEqual(
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab "
            "WHERE a = 'val'")),
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab /* oh\n"
            "hai */ WHERE a = 'val'"))
    ).

multiline_comment_with_asterisk_inside_in_select_test() ->
    ?assertEqual(
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab "
            "WHERE a = 'val'")),
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab /** *\n"
            " * / / *\n"
            "hai */ WHERE a = 'val'"))
    ).

multiple_multiline_comment_in_select_test() ->
    ?assertEqual(
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab "
            "WHERE a = 'val'")),
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab /* oh\n"
            "hai */ /* hi again */ WHERE a = 'val'"))
    ).

single_line_comment_in_select_test() ->
    ?assertEqual(
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab "
            "WHERE a = 'val'")),
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab WHERE a = 'val' -- a comment"))
    ).

single_line_comment_in_multiline_select_test() ->
    ?assertEqual(
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab "
            "WHERE a = 'val'")),
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab -- a comment\n"
            "WHERE a = 'val'"))
    ).


single_line_comment_in_multiline_ctrl_select_test() ->
    ?assertEqual(
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab "
            "WHERE a = 'val'")),
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab -- a comment\r\n"
            "WHERE a = 'val'"))
    ).

single_line_comment_single_line_in_multiline_comment_select_test() ->
    ?assertEqual(
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab "
            "WHERE a = 'val'")),
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
            "SELECT * FROM mytab "
            "/*\n"
            "Some text -- */\n"
            "*/\n"
            "WHERE a = 'val'"))
    ).

