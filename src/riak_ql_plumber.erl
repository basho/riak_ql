%% -------------------------------------------------------------------
%%
%% riak_ql_plumber - rudimentary plumber, putting together pfittings from an
%% AST.
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
-module(riak_ql_plumber).
-export([create/1,
         open/1]).

-type ast() :: proplists:proplist().

-spec create(ast()) -> [riak_ql_pfitting:pfitting()|riak_ql_pspout:pspout()]
                       |{error, string()}.
create([]) ->
    {error, "Incomplete AST provided."};
create(Ast) ->
    create1(extract_table(Ast), Ast).

create1(undefined, _Ast) ->
    {error, "AST missing table(s)"};
create1(_Table, Ast) ->
    [{project, Project}, {fields, Fields}] = extract_project(Ast),
    Ast1 = replace_fields(Fields, Ast),
    Spout = extract_pspout(Ast1),
    Arithmetic = extract_arithmetic(Ast),
    Aggregate = extract_aggregate(Ast),
    [Spout,
     Project,
     Arithmetic,
     Aggregate].

-spec open(proplists:proplist()) -> riak_ql_pfitting_process_result:process_result().
open(Pipeline) ->
    open(Pipeline, []).
open([], Acc) ->
    Acc;
open([undefined|T], Acc) ->
    open(T, Acc);
open(Pipeline = [Ppart|_T], Acc) ->
    case element(1, Ppart) of
        pspout -> open_pspout(Pipeline, Acc);
        pfitting -> open_pfitting(Pipeline, Acc)
    end.

open_pspout([Pspout|T], _Acc) ->
    Res = riak_ql_pspout:open(Pspout),
    open(T, Res).

open_pfitting([Pfitting|T], Acc) ->
    Res = riak_ql_pfitting:process(Pfitting, Acc),
    open(T, Res).

extract_project(Ast) ->
    Fields = extract_fields(Ast),
    extract_project1(Fields).

extract_project1(undefined) ->
    {error, "AST missing fields"};
extract_project1(Fields = [<<"*">>]) ->
    [{project, undefined}, {fields, Fields}];
extract_project1(Fields) ->
    ColumnMappings = [riak_ql_pfitting_project:create_projection_column_mapping(Identifier) ||
                      Identifier <- Fields],
    [{project, riak_ql_pfitting:create(riak_ql_pfitting_project, ColumnMappings)},
     {fields, Fields}].

extract_pspout(Ast) ->
    riak_ql_pspout_mock:create(Ast).

extract_arithmetic(_Ast) ->
    undefined. %% TODO

extract_aggregate(_Ast) ->
    undefined. %% TODO

extract_table(Ast) ->
    proplists:get_value(tables, Ast,
                        proplists:get_value(table, Ast)).

extract_fields(Ast) ->
    proplists:get_value(fields, Ast).

replace_fields(Fields, Ast) ->
    lists:keyreplace(fields, 1, Ast, {fields, Fields}).
