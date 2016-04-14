%% -------------------------------------------------------------------
%%
%% More WHERE clause tests for the Parser
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

-module(parser_select_where_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

select_where_1_sql_test() ->
    ?sql_comp_assert_match("select value from response_times "
                           "where time > '2013-08-12 23:32:01' and time < '2013-08-13 12:34:56'", select,
                           [{fields, [
                                      {identifier, [<<"value">>]}
                                     ]},
                            {tables, <<"response_times">>},
                            {where, [
                                     {and_,
                                      {'>', <<"time">>, {binary, <<"2013-08-12 23:32:01">>}}
                                     }
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_1_reverse_sql_test() ->
    ?sql_comp_assert_match("select value from response_times "
                           "where '2013-08-12 23:32:01' < time and '2013-08-13 12:34:56' > time", select,
                           [{fields, [
                                      {identifier, [<<"value">>]}
                                     ]},
                            {tables, <<"response_times">>},
                            {where, [
                                     {and_,
                                      {'<', <<"time">>, {binary,<<"2013-08-13 12:34:56">>}},
                                      {'>', <<"time">>, {binary, <<"2013-08-12 23:32:01">>}}
                                     }
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_3_sql_test() ->
    ?sql_comp_assert_match("select value from response_times where time > 1388534400", select,
                           [{fields, [
                                      {identifier, [<<"value">>]}
                                     ]},
                            {tables, <<"response_times">>},
                            {where, [
                                     {'>', <<"time">>, {integer, 1388534400}}
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_4_sql_test() ->
    ?sql_comp_assert_match("select value from response_times where time > 1388534400s", select,
                           [{fields, [
                                      {identifier, [<<"value">>]}
                                     ]},
                            {tables, <<"response_times">>},
                            {where, [
                                     {'>', <<"time">>, {integer, 1388534400000}}
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_5_sql_test() ->
    ?sql_comp_assert_match("select * from events where time = 1400497861762723 "
                           "and sequence_number = 2321", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {tables, <<"events">>},
                            {where, [
                                     {and_,
                                      {'=', <<"sequence_number">>, {integer, 2321}},
                                      {'=', <<"time">>,            {integer, 1400497861762723}}
                                     }
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_8_sql_test() ->
    ?sql_comp_assert_match("select * from events where state = 'NY'", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {tables, <<"events">>},
                            {where, [
                                     {'=', <<"state">>, {binary, <<"NY">>}}
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_approxmatch_sql_test() ->
    ?sql_comp_fail("select * from log_lines where line =~ /error/i").

select_where_10_sql_test() ->
    ?sql_comp_assert_match("select * from events where customer_id = 23 and type = 'click10'", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {tables, <<"events">>},
                            {where, [
                                     {and_,
                                      {'=', <<"customer_id">>, {integer,  23}},
                                      {'=', <<"type">>,        {binary, <<"click10">>}}
                                     }
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_11_sql_test() ->
    ?sql_comp_assert_match("select * from response_times where value > 500", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {tables, <<"response_times">>},
                            {where, [
                                     {'>', <<"value">>, {integer, 500}}
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_11a_sql_test() ->
    ?sql_comp_assert_match("select * from response_times where value >= 500", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {tables, <<"response_times">>},
                            {where, [
                                     {'>=', <<"value">>, {integer, 500}}
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_11b_sql_test() ->
    ?sql_comp_assert_match("select * from response_times where value <= 500", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {tables, <<"response_times">>},
                            {where, [
                                     {'<=', <<"value">>, {integer, 500}}
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_not_approx_sql_test() ->
    ?sql_comp_fail("select * from events where email !~ /.*gmail.*/").

select_where_ne_sql_test() ->
    ?sql_comp_fail("select * from nagios_checks where status <> 0").

select_where_14_sql_test() ->
    ?sql_comp_assert_match("select * from events where signed_in = false", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {tables, <<"events">>},
                            {where, [
                                     {'=', <<"signed_in">>, {boolean, false}}
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_15_sql_test() ->
    ?sql_comp_assert_match("select * from events where signed_in = -3", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {tables, <<"events">>},
                            {where, [
                                     {'=', <<"signed_in">>, {integer, -3}}
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_approx_or_approx_sql_test() ->
    ?sql_comp_fail("select * from events where (email =~ /.*gmail.*/ or " ++
                       "email =~ /.*yahoo.*/) and state = 'ny'").

select_where_letters_nos_in_strings_1a_test() ->
    ?sql_comp_assert_match("select * from events where user = 'user 1'", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {tables, <<"events">>},
                            {where, [
                                     {'=', <<"user">>, {binary, <<"user 1">>}}
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_letters_nos_in_strings_2a_test() ->
    ?sql_comp_assert_match(
       "select weather from GeoCheckin where time > 2000 and time < 8000 and user = 'user_1'", select,
       [{fields, [
                  {identifier, [<<"weather">>]}
                 ]},
        {tables, <<"GeoCheckin">>},
        {where, [
                 {and_,
                  {'=', <<"user">>, {binary, <<"user_1">>}},
                  {and_,
                   {'<', <<"time">>, {integer, 8000}},
                   {'>', <<"time">>, {integer, 2000}}
                  }
                 }
                ]}
       ],
       {query_compiler, 1}, {query_coordinator, 1}).

select_where_single_quotes_test() ->
    ?sql_comp_assert_match(
       "select weather from GeoCheckin where user = 'user_1' and location = 'San Francisco'", select,
       [{fields, [
                  {identifier, [<<"weather">>]}
                 ]},
        {tables, <<"GeoCheckin">>},
        {where, [
                 {and_,
                  {'=', <<"location">>, {binary, <<"San Francisco">>}},
                  {'=', <<"user">>, {binary, <<"user_1">>}}
                 }
                ]}
       ],
       {query_compiler, 1}, {query_coordinator, 1}).

select_where_ors_at_start_test() ->
    ?sql_comp_assert_match(
       "select * FROM tsall2 WHERE "
       "d3 = 1.0 OR d3 = 2.0 "
       "AND vc1nn != '2' AND vc2nn != '3' AND 0 < ts1nn  AND ts1nn < 1", select,
       [{fields, [
                  {identifier, [<<"*">>]}
                 ]},
        {tables, <<"tsall2">>},
        {where, [
                 {or_,
                  {'=', <<"d3">>, {float, 1.0}},
                  {and_,
                   {'<', <<"ts1nn">>, {integer, 1}},
                   {and_,
                    {'>', <<"ts1nn">>, {integer, 0}},
                    {and_,
                     {'!=', <<"vc2nn">>, {binary, <<"3">>}},
                     {and_,
                      {'!=', <<"vc1nn">>, {binary, <<"2">>}},
                      {'=', <<"d3">>, {float, 2.0}}
                     }}}}}
                ]}
       ],
       {query_compiler, 1}, {query_coordinator, 1}).

select_where_ors_at_end_test() ->
    ?sql_comp_assert_match(
       "select * FROM tsall2 WHERE "
       "d3 = 1.0 OR d3 = 2.0 "
       "AND vc1nn != '2' AND vc2nn != '3' AND 0 < ts1nn  AND ts1nn < 1 "
       "OR d3 = 3.0 OR d3 = 4.0", select,
       [{fields, [
                  {identifier, [<<"*">>]}
                 ]},
        {tables, <<"tsall2">>},
        {where, [
                 {or_,
                  {'=',<<"d3">>,{float,4.0}},
                  {or_,
                   {'=',<<"d3">>,{float,3.0}},
                   {or_,
                    {'=', <<"d3">>, {float, 1.0}},
                    {and_,
                     {'<', <<"ts1nn">>, {integer, 1}},
                     {and_,
                      {'>', <<"ts1nn">>, {integer, 0}},
                      {and_,
                       {'!=', <<"vc2nn">>, {binary, <<"3">>}},
                       {and_,
                        {'!=', <<"vc1nn">>, {binary, <<"2">>}},
                        {'=', <<"d3">>, {float, 2.0}}
                       }}}}}}}
                ]}
       ],
       {query_compiler, 1}, {query_coordinator, 1}).

select_where_letters_nos_in_strings_2b_test() ->
    ?sql_comp_assert_match("select weather from GeoCheckin where time > 2000 and time < 8000 and user = 'user_1'", select,
                           [{fields, [
                                      {identifier, [<<"weather">>]}
                                     ]},
                            {tables, <<"GeoCheckin">>},
                            {where, [
                                     {and_,
                                      {'=', <<"user">>, {binary, <<"user_1">>}},
                                      {and_,
                                       {'<', <<"time">>, {integer, 8000}},
                                       {'>', <<"time">>, {integer, 2000}}
                                      }
                                     }
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_brackets_1_test() ->
    ?sql_comp_assert_match("select weather from GeoCheckin where (time > 2000 and time < 8000) and user = 'user_1'", select,
                           [{fields, [
                                      {identifier, [<<"weather">>]}
                                     ]},
                            {tables, <<"GeoCheckin">>},
                            {where, [
                                     {and_,
                                      {'=', <<"user">>, {binary, <<"user_1">>}},
                                      {and_,
                                       {'<', <<"time">>, {integer, 8000}},
                                       {'>', <<"time">>, {integer, 2000}}
                                      }
                                     }
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_brackets_2_test() ->
    ?sql_comp_assert_match("select weather from GeoCheckin where user = 'user_1' and (time > 2000 and time < 8000)", select,
                           [{fields, [
                                      {identifier, [<<"weather">>]}
                                     ]},
                            {tables, <<"GeoCheckin">>},
                            {where, [
                                     {and_,
                                      {'=', <<"user">>, {binary, <<"user_1">>}},
                                      {and_,
                                       {'<', <<"time">>, {integer, 8000}},
                                       {'>', <<"time">>, {integer, 2000}}
                                      }
                                     }
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_where_brackets_2a_test() ->
    ?sql_comp_assert_match("select weather from GeoCheckin where user = 'user_1' and (time > 2000 and (time < 8000))", select,
                           [{fields, [
                                      {identifier, [<<"weather">>]}
                                     ]},
                            {tables, <<"GeoCheckin">>},
                            {where, [
                                     {and_,
                                      {'=', <<"user">>, {binary, <<"user_1">>}},
                                      {and_,
                                       {'<', <<"time">>, {integer, 8000}},
                                       {'>', <<"time">>, {integer, 2000}}
                                      }
                                     }
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).

select_field_to_field_forbidden_test() ->
    ?sql_comp_fail("select * from table where time = time").

select_quoted_where_sql_test() ->
    ?sql_comp_assert_match("select * from \"table with spaces\" where \"color spaces\" = 'someone had painted it blue'", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {tables, <<"table with spaces">>},
                            {where, [
                                     {'=', <<"color spaces">>, {binary, <<"someone had painted it blue">>}}
                                    ]}
                           ],
                           {query_compiler, 1}, {query_coordinator, 1}).
