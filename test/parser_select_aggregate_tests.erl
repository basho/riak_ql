%% -------------------------------------------------------------------
%%
%% Window Aggregation function tests for the Parser
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

-module(parser_select_aggregate_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

window_aggregate_fn_1_test() ->
    ?sql_comp_assert_match(
       "select avg(temp) from details", select,
       [{fields, [
                  {{window_agg_fn, 'AVG'},
                   [{identifier, [<<"temp">>]}]}
                 ]},
        {tables, <<"details">>}
       ]
      ).

window_aggregate_fn_1a_test() ->
    ?sql_comp_assert_match(
       "select mean(temp) from details", select,
       [{fields, [
                  {{window_agg_fn, 'MEAN'},
                   [{identifier, [<<"temp">>]}]}
                 ]},
        {tables, <<"details">>}
       ]
      ).

window_aggregate_fn_2_test() ->
    ?sql_comp_assert_match(
       "select avg(temp), sum(counts) from details", select,
       [{fields, [
                  {{window_agg_fn, 'AVG'},
                   [{identifier, [<<"temp">>]}]},
                  {{window_agg_fn, 'SUM'},
                   [{identifier, [<<"counts">>]}]}
                 ]},
        {tables, <<"details">>}
       ]
      ).

window_aggregate_fn_wildcard_count_test() ->
    ?sql_comp_assert_match(
       "select count(*) from details", select,
       [{fields, [
                  {{window_agg_fn, 'COUNT'},
                   [{identifier, [<<"*">>]}]}
                 ]},
        {tables, <<"details">>}
       ]
      ).

window_aggregate_fn_capitalisation_test() ->
    ?sql_comp_assert_match(
       "select aVg(temp) from details", select,
       [{fields, [
                  {{window_agg_fn, 'AVG'},
                   [{identifier, [<<"temp">>]}]}
                 ]},
        {tables, <<"details">>}
       ]
      ).

window_aggregate_fn_all_funs_test() ->
    ?sql_comp_assert_match(
       "select avg(temp), sum(counts), count(counts), min(counts), "
       "max(counts), stddev(counts) from details", select,
       [{fields, [
                  {{window_agg_fn, 'AVG'},
                   [{identifier, [<<"temp">>]}]},
                  {{window_agg_fn, 'SUM'},
                   [{identifier, [<<"counts">>]}]},
                  {{window_agg_fn, 'COUNT'},
                   [{identifier, [<<"counts">>]}]},
                  {{window_agg_fn, 'MIN'},
                   [{identifier, [<<"counts">>]}]},
                  {{window_agg_fn, 'MAX'},
                   [{identifier, [<<"counts">>]}]},
                  {{window_agg_fn, 'STDDEV'},
                   [{identifier, [<<"counts">>]}]}
                 ]},
        {tables, <<"details">>}
       ]
      ).


window_aggregate_fn_arithmetic_2_test() ->
    ?sql_comp_assert_match(
       "select aVg(temperature) + count(temperature) from details", select,
       [{fields, [
                  {'+',
                   {{window_agg_fn, 'AVG'},
                    [{identifier, [<<"temperature">>]}]},
                   {{window_agg_fn, 'COUNT'},
                    [{identifier, [<<"temperature">>]}]}}
                 ]},
        {tables, <<"details">>}
       ]
      ).

window_aggregate_fn_arithmetic_3_test() ->
    ?sql_comp_assert_match(
       "select aVg(temperature + 1) + count(temperature / distance) from details", select,
       [{fields, [
                  {'+',
                   {{window_agg_fn, 'AVG'}, [{'+', {identifier, <<"temperature">>}, {integer, 1}}]},
                   {{window_agg_fn, 'COUNT'}, [{'/', {identifier, <<"temperature">>}, {identifier, <<"distance">>}}]}
                  }]
        },
        {tables, <<"details">>}
       ]
      ).

%%
%% TS 1.1 fail tests
%%

window_aggregate_fn_not_supported_test() ->
    ?sql_comp_fail("select bingo(temp) from details").

window_aggregate_fn_wildcard_fail_test() ->
    ?sql_comp_fail("select avg(*) from details").
