%% -------------------------------------------------------------------
%%
%% riak_ql_pspout_mock -> mock pipeline spout.
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
-module(riak_ql_pspout_mock).
-behaviour(riak_ql_pspout).
-export([create/1]).
-export([open_valid/0,
         open_invalid/0]).

-spec create(proplists:proplist()) -> riak_ql_pspout:pspout().
create(Ast) ->
    {Module, OpenFn} = open(Ast),
    riak_ql_pspout:create(Module, OpenFn).

open(Ast) ->
    case extract_table_name(Ast) of
        undefined -> {?MODULE, fun open_invalid/0};
        _ -> {?MODULE, fun open_valid/0}
    end.

extract_table_name([]) -> undefined;
extract_table_name([{tables, Table}|_T]) -> Table;
extract_table_name([{table, Table}|_T]) -> Table;
extract_table_name([_H|T]) -> extract_table_name(T).

open_invalid() ->
    Columns = [],
    Rows = [],
    Errors = [{error, <<"No table specified.">>}],
    riak_ql_pfitting_process_result:create(Columns, Rows, Errors).

open_valid() ->
    Columns = [<<"r">>, <<"i">>, <<"a">>, <<"k">>, <<"ts">>],
    Rows = [[1.0,  1,   <<"one">>, true, 1000],
            [ [], [],          [],   [],   []],
            [ [],  2,   <<"two">>, true, 2000],
            [3.0, [], <<"three">>, true, 3000],
            [4.0,  4,          [], true, 4000],
            [5.0,  5,  <<"five">>, true,  []]],
    Errors = [],
    riak_ql_pfitting_process_result:create(Columns, Rows, Errors).
