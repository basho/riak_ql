%% -------------------------------------------------------------------
%%
%% Arithmetic tests for the Parser
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
-module(parser_arithmetic_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

select_arithmetic_test() ->
    ?sql_comp_assert_match("select temperature + 1 from details", select,
                           [{fields, [
                                      {'+',
                                       {identifier, <<"temperature">>},
                                       {integer, 1}
                                      }
                                     ]},
                            {tables, <<"details">>},
                            {where, []}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

arithmetic_precedence_test() ->
    ?sql_comp_assert_match("select 1 * 2 + 3 / 4 - 5 * 6 from dual", select,
                           [{fields,
                             [{'-',
                               {'+',
                                {'*', {integer,1}, {integer,2}},
                                {'/', {integer,3}, {integer,4}}
                               },
                               {'*', {integer,5}, {integer,6}}
                              }]},
                            {tables, <<"dual">>},
                            {where, []}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

parens_precedence_test() ->
    ?sql_comp_assert_match("select 1 * (2 + 3) / (4 - 5) * 6 from dual", select,
                           [{fields,
                             [{'*',
                               {'/',
                                {'*', {integer,1},
                                 {'+',{integer,2}, {integer,3}}},
                                {'-',{integer,4}, {integer,5}}},
                               {integer,6}}]},
                            {tables, <<"dual">>},
                            {where, []}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

negated_parens_test() ->
    ?sql_comp_assert_match("select - (2 + 3) from dual", select,
                           [{fields,
                             [{negate,
                               {expr,
                                {'+', {integer,2}, {integer,3}}}}
                             ]},
                            {tables, <<"dual">>},
                            {where, []}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

no_functions_in_where_test() ->
    ?sql_comp_fail("select * from dual where sin(4) > 4").

window_aggregate_fn_arithmetic_1_test() ->
    ?sql_comp_assert_match(
       "SELECT AVG(temperature) + 1 - 2 * 3 / 4 FROM details", select,
       [{fields,
         [{'-',{'+',{{window_agg_fn,'AVG'},
                     [{identifier,[<<"temperature">>]}]
                    },{integer,1}},
           {'/',{'*',{integer,2},{integer,3}},{integer,4}}}]
        },
        {tables, <<"details">>}],
       {query_compiler, 2}, {query_coordinator, 1}).

window_aggregate_fn_arithmetic_2_test() ->
    ?sql_comp_assert_match(
       "SELECT AVG((temperature * 2) + 32) FROM details", select,
       [{fields,
         [{{window_agg_fn,'AVG'},
           [{'+',{expr,{'*',
                        {identifier,<<"temperature">>},{integer,2}}},{integer,32}}]}]
        },
        {tables, <<"details">>}],
       {query_compiler, 2}, {query_coordinator, 1}).

window_aggregate_fn_arithmetic_3_test() ->
    ?sql_comp_assert_match(
       "SELECT COUNT(x) + 1 / AVG(y) FROM details", select,
       [{fields,
         [{'+',{{window_agg_fn,'COUNT'},
                [{identifier,[<<"x">>]}]},
           {'/',{integer,1},
            {{window_agg_fn,'AVG'},[{identifier,[<<"y">>]}]}}}]},
        {tables, <<"details">>}],
       {query_compiler, 2}, {query_coordinator, 1}).
