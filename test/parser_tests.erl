%% -------------------------------------------------------------------
%%
%% General Parser Tests
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

-module(parser_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").


select_sql_case_insensitive_1_test() ->
    ?sql_comp_assert_match("SELECT * from argle", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]}
                           ]).

select_sql_case_insensitive_2_test() ->
    ?sql_comp_assert_match("seLEct * from argle", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]}
                           ]).

sql_first_char_is_newline_test() ->
    ?sql_comp_assert_match("\nselect * from argle", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]}
                           ]).

%% RTS-645
flubber_test() ->
    ?assertEqual(
       {error, {0, riak_ql_parser,
                <<"Used f as a measure of time in 1f. Only s, m, h and d are allowed.">>}},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM ts_X_subquery "
                                 "WHERE d > 0 AND d < 1 f = 'f' "
                                 "AND s='s' AND ts > 0 AND ts < 100"))
      ).

time_unit_seconds_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10s AND time < 20s", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, [
                                     {and_,
                                      {'<',<<"time">>,{integer,20 * 1000}},
                                      {'>',<<"time">>,{integer,10 * 1000}}}
                                    ]}
                           ]).

time_unit_minutes_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10m AND time < 20m", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, [
                                     {and_,
                                      {'<',<<"time">>,{integer,20 * 60 * 1000}},
                                      {'>',<<"time">>,{integer,10 * 60 * 1000}}}
                                    ]}
                           ]).

time_unit_seconds_and_minutes_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10s AND time < 20m", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, [
                                     {and_,
                                      {'<',<<"time">>,{integer,20 * 60 * 1000}},
                                      {'>',<<"time">>,{integer,10 * 1000}}}
                                    ]}
                           ]).

time_unit_hours_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10h AND time < 20h", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, [
                                     {and_,
                                      {'<',<<"time">>,{integer,20 * 60 * 60 * 1000}},
                                      {'>',<<"time">>,{integer,10 * 60 * 60 * 1000}}}
                                    ]}
                           ]).

time_unit_days_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10d AND time < 20d", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, [
                                     {and_,
                                      {'<',<<"time">>,{integer,20 * 60 * 60 * 24 * 1000}},
                                      {'>',<<"time">>,{integer,10 * 60 * 60 * 24 * 1000}}}
                                    ]}
                           ]).

time_unit_invalid_1_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytable WHERE time > 10y AND time < 20y"))
      ).

time_unit_invalid_2_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytable WHERE time > 10mo AND time < 20mo"))
      ).

time_unit_whitespace_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10   d AND time < 20\td", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, [
                                     {and_,
                                      {'<',<<"time">>,{integer,20 * 60 * 60 * 24 * 1000}},
                                      {'>',<<"time">>,{integer,10 * 60 * 60 * 24 * 1000}}}
                                    ]}
                           ]).

time_unit_case_insensitive_test() ->
    ?assertMatch(
       {select, _},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytable WHERE time > 10S "
                                 "AND time < 20M AND time > 15H and time < 4D"))
      ).

left_hand_side_literal_equals_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE 10 = age", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, [
                                     {'=', <<"age">>, {integer, 10}}
                                    ]}
                           ]).

left_hand_side_literal_not_equals_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE 10 != age", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, [
                                     {'!=', <<"age">>, {integer, 10}}
                                    ]}
                           ]).

%% RTS-788
%% an infinite loop was occurring when two where clauses were the same
%% i.e. time = 10 and time 10
infinite_loop_test_() ->
    {timeout, 0.2,
     fun() ->
             ?assertMatch(
                {select, _},
                riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                          "Select myseries, temperature from GeoCheckin2 "
                                          "where time > 1234567 and time > 1234567 "
                                          "and myfamily = 'family1' and myseries = 'series1' "))
               )
     end}.

remove_duplicate_clauses_1_test() ->
    ?assertEqual(
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytab "
                                 "WHERE time > 1234567 ")),
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytab "
                                 "WHERE time > 1234567 AND time > 1234567"))
      ).

remove_duplicate_clauses_2_test() ->
    ?assertEqual(
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytab "
                                 "WHERE time > 1234567 ")),
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytab "
                                 "WHERE time > 1234567 AND time > 1234567 AND time > 1234567 "))
      ).

remove_duplicate_clauses_3_test() ->
    ?assertEqual(
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytab "
                                 "WHERE time > 1234567 ")),
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytab "
                                 "WHERE time > 1234567 AND time > 1234567 OR time > 1234567 "))
      ).

remove_duplicate_clauses_4_test() ->
    ?assertEqual(
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytab "
                                 "WHERE time > 1234567 ")),
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
                                 "SELECT * FROM mytab "
                                 "WHERE time > 1234567 AND (time > 1234567 OR time > 1234567) "))
      ).

%% This fails. de-duping does not yet go through the entire tree and
%% pull out duplicates
%% remove_duplicate_clauses_5_test() ->
%%   ?assertEqual(
%%         riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
%%             "SELECT * FROM mytab "
%%             "WHERE time > 1234567 "
%%             "AND (localtion > 'derby' OR time > 'sheffield') "
%%             "AND weather = 'raining' ")),
%%         riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
%%             "SELECT * FROM mytab "
%%             "WHERE time > 1234567 "
%%             "AND (localtion > 'derby' OR time > 'sheffield') "
%%             "AND weather = 'raining' "
%%             "AND time > 1234567 "))
%%     ).

concatenated_unquoted_strings_test() ->
    String = "select * from response_times where cats = be a st",
    Expected = error,
    Got = case riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(String)) of
              {error, _Err} ->
                  error;
              {_, Other} -> {should_not_compile, Other}
          end,
    ?assertEqual(Expected, Got).

%%
%% Regression tests
%%

rts_433_regression_test() ->
    ?sql_comp_assert_match("select * from HardDrivesV14 where date >= 123 "
                           "and date <= 567 "
                           "and family = 'Hitachi HDS5C4040ALE630' "
                           "and series = 'true'", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, [
                                     {and_,
                                      {'=', <<"series">>, {binary, <<"true">>}},
                                      {and_,
                                       {'=', <<"family">>, {binary, <<"Hitachi HDS5C4040ALE630">>}},
                                       {and_,
                                        {'<=', <<"date">>, {integer, 567}},
                                        {'>=', <<"date">>, {integer, 123}}
                                       }
                                      }
                                     }
                                    ]}
                           ]).
