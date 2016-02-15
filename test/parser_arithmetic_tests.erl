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
-include("riak_ql_ddl.hrl").

-define(sql_comp_assert(String, Expected),
        Exp2 = fix(Expected),
        Toks = riak_ql_lexer:get_tokens(String),
 %% io:format(standard_error, "~p~n~p~n", [Exp2, Got]),
        ?assertEqual({ok, Exp2},  riak_ql_parser:parse(Toks))).

fix(?SQL_SELECT{'FROM' = F} = Expected) ->
    case F of
        {regex, _} -> Expected;
        {list,  _} -> Expected;
        _          -> Mod = riak_ql_ddl:make_module_name(F),
                      Expected?SQL_SELECT{helper_mod = Mod}
    end;
fix(Other) ->
    Other.

-define(sql_comp_fail(QL_string),
        Toks = riak_ql_lexer:get_tokens(QL_string),
        Got = riak_ql_parser:parse(Toks),
        ?assertMatch({error, _}, Got)).

select_arithmetic_test() ->
    ?sql_comp_assert("select temperature + 1 from details",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                              {'+',
                                               {identifier, <<"temperature">>},
                                               {integer, 1}
                                              }
                                             ]},
                                  'FROM'    = <<"details">>,
                                  'WHERE'   = []
                                 }).

arithmetic_precedence_test() ->
    ?sql_comp_assert("select 1 * 2 + 3 / 4 - 5 * 6 from dual",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{'-',
                                               {'+',
                                                {'*', {integer,1}, {integer,2}},
                                                {'/', {integer,3}, {integer,4}}
                                               },
                                               {'*', {integer,5}, {integer,6}}
                                               }]},
                                  'FROM' = <<"dual">>,
                                  'WHERE' = []
                                 }).

parens_precedence_test() ->
    ?sql_comp_assert("select 1 * (2 + 3) / (4 - 5) * 6 from dual",
                     ?SQL_SELECT{
                        'SELECT' =
                            #riak_sel_clause_v1{
                               clause = [{'*',
                                          {'/',
                                           {'*', {integer,1},
                                            {'+',{integer,2}, {integer,3}}},
                                           {'-',{integer,4}, {integer,5}}},
                                          {integer,6}}]},
                                  'FROM' = <<"dual">>,
                                  'WHERE' = []
                                 }).

negated_parens_test() ->
        ?sql_comp_assert("select - (2 + 3) from dual",
                     ?SQL_SELECT{
                        'SELECT' =
                            #riak_sel_clause_v1{
                               clause = [{negate,
                                          {expr,
                                           {'+', {integer,2}, {integer,3}}}}
                                        ]},
                                  'FROM' = <<"dual">>,
                                  'WHERE' = []
                                 }).

no_functions_in_where_test() ->
    ?sql_comp_fail("select * from dual where sin(4) > 4").

window_aggregate_fn_arithmetic_1_test() ->
    ?sql_comp_assert(
        "SELECT AVG(temperature) + 1 - 2 * 3 / 4 FROM details",
        ?SQL_SELECT{
            'SELECT' =
                #riak_sel_clause_v1{
                  clause = [{'-',{'+',{{window_agg_fn,'AVG'},[{identifier,[<<"temperature">>]}]},{integer,1}},
                                             {'/',{'*',{integer,2},{integer,3}},{integer,4}}}]
                },
            'FROM' = <<"details">>
    }).

window_aggregate_fn_arithmetic_2_test() ->
    ?sql_comp_assert(
        "SELECT AVG((temperature * 2) + 32) FROM details",
        ?SQL_SELECT{
           'SELECT' =
               #riak_sel_clause_v1{
                  clause = [{{window_agg_fn,'AVG'},[{'+',{expr,{'*',{identifier,<<"temperature">>},{integer,2}}},{integer,32}}]}]
                                             },
                     'FROM' = <<"details">>
                    }).

window_aggregate_fn_arithmetic_3_test() ->
    ?sql_comp_assert(
        "SELECT COUNT(x) + 1 / AVG(y) FROM details",
        ?SQL_SELECT{
           'SELECT' =
               #riak_sel_clause_v1{
                  clause = [{'+',{{window_agg_fn,'COUNT'},[{identifier,[<<"x">>]}]},{'/',{integer,1},{{window_agg_fn,'AVG'},[{identifier,[<<"y">>]}]}}}]
                                             },
                     'FROM' = <<"details">>
                    }).
