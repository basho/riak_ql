%% -------------------------------------------------------------------
%%
%% riak_ql_pfitting_project: Riak Query Pipeline pipe fitting (pfitting)
%% for projection (reducing and/or reordering columns). Multiple row result
%% sets are process to have a column containing the value of the corresponding
%% column in the origin row for each column mapping defined for the pfitting.
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
-module(riak_ql_pfitting_project).
-behaviour(riak_ql_pfitting).

-export([process/3]).
-export([create_projection_column_mapping/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. %%TEST

-spec create_projection_column_mapping({identifier,[binary()]}) -> riak_ql_pfitting_column_mapping:column_mapping().
create_projection_column_mapping(ColumnIdentifier = {identifier, [Column]}) ->
    riak_ql_pfitting_column_mapping:create(ColumnIdentifier,
                                           Column,
                                           unresolved);
create_projection_column_mapping({identifier, Column}) ->
    create_projection_column_mapping({identifier, [Column]});
create_projection_column_mapping(Column) ->
    create_projection_column_mapping({identifier, [Column]}).

process(Pfitting, Columns, Rows) ->
    ColumnMappings = riak_ql_pfitting:get_column_mappings(Pfitting),
    Res = process_projection(Columns, Rows, ColumnMappings),
    ResStatus = riak_ql_pfitting_process_result:get_status(Res),
    {ResStatus, Res}.

project_row(Row, Columns, ColumnMappingIdentifiers) ->
    project_row_(Row, Columns, ColumnMappingIdentifiers, Row, Columns, []).
project_row_(Row, Columns, Columns, _Row, _Columns, []) ->
    Row;
project_row_([], _Columns, _ColumnMappingIdentifiers, _ORow, _OColumns, Acc) ->
    lists:reverse(Acc);
project_row_(_Row, _Columns, [], _ORow, _OColumns, Acc) ->
    lists:reverse(Acc);
project_row_([RowV|_RowT], [Column|_ColumnT], [Column|ColumnMappingIdentifiersT], ORow, OColumns, Acc) ->
    project_row_(ORow, OColumns, ColumnMappingIdentifiersT, ORow, OColumns, [RowV|Acc]);
project_row_([_RowV|RowT], [_Column|ColumnT], ColumnMappingIdentifiers, ORow, OColumns, Acc) ->
    project_row_(RowT, ColumnT, ColumnMappingIdentifiers, ORow, OColumns, Acc).

assert_column_mappings(Columns, ColumnMappings) ->
    assert_column_mappings_(Columns, ColumnMappings, Columns).
assert_column_mappings_(_Columns, [], _Columns) ->
    pass;
assert_column_mappings_([], [ColumnMappingH|_ColumnMappingT], _Columns) ->
    throw({invalid_column_mapping, ColumnMappingH});
assert_column_mappings_([ColumnH|_ColumnT], [ColumnH|ColumnMappingT], Columns) ->
    assert_column_mappings_(Columns, ColumnMappingT, Columns);
assert_column_mappings_([_ColumnH|ColumnT], ColumnMappings, Columns) ->
    assert_column_mappings_(ColumnT, ColumnMappings, Columns).

process_projection(Columns, Rows, ColumnMappings) ->
    ProcessColumns = [riak_ql_pfitting_column_mapping:get_input_identifier(ColumnMapping) ||
                         ColumnMapping <- ColumnMappings],
    ProcessColumns1 = [Column || {identifier, [Column]} <- ProcessColumns],
    {ProcessRows, ProcessErrors} = try
                         assert_column_mappings(Columns, ProcessColumns1),
                         {[project_row(Row, Columns, ProcessColumns1) ||
                          Row <- Rows],
                          []}
                     catch
                         throw:{invalid_column_mapping, ColumnMapping} ->
                                           {[], [{invalid_column_mapping, ColumnMapping}]}
                     end,
    riak_ql_pfitting_process_result:create(ProcessColumns1,
                                           ProcessRows, ProcessErrors).

-ifdef(TEST).

lex_parse(Sql) ->
    riak_ql_parser:parse(riak_ql_lexer:get_tokens(Sql)).

process_setup(ColumnIdentifiers) ->
    ColumnMappings = [create_projection_column_mapping(ColumnIdentifier) ||
                      ColumnIdentifier <- ColumnIdentifiers],
    Pfitting = riak_ql_pfitting:create(?MODULE, ColumnMappings),
    {ok, Ast} = lex_parse("select r, i, a, k, ts from mock where ts > 0 and ts < 10"),
    Pspout = riak_ql_pspout_mock:create(Ast),
    Res = riak_ql_pspout:open(Pspout),
    Rows = riak_ql_pfitting_process_result:get_rows(Res),
    {Pfitting, Res, Rows}.

process_projection_empty_test() ->
    ProjectedColumns = [],
    {Pfitting, Res, Rows} = process_setup(ProjectedColumns),
    ExpectedRows = [[] || _Row <- Rows],
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pfitting_process_result:create(ProjectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_projection_same_test() ->
    ProjectedColumns = [<<"r">>,<<"i">>,<<"a">>,<<"k">>,<<"ts">>],
    {Pfitting, Res, Rows} = process_setup(ProjectedColumns),
    ExpectedRows = Rows,
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pfitting_process_result:create(ProjectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_projection_rearranged_test() ->
    ProjectedColumns = [<<"i">>,<<"r">>,<<"k">>,<<"a">>],
    {Pfitting, Res, Rows} = process_setup(ProjectedColumns),
    ExpectedRows = [[V1,V0,V3,V2] ||
                    [V0,V1,V2,V3,_V4] <- Rows],
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pfitting_process_result:create(ProjectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_projection_reduced_test() ->
    ProjectedColumns = [<<"r">>,<<"a">>,<<"i">>],
    {Pfitting, Res, Rows} = process_setup(ProjectedColumns),
    ExpectedRows = [[V0,V2,V1] ||
                    [V0,V1,V2,_V3,_V4] <- Rows],
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pfitting_process_result:create(ProjectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_projection_invalid_identifier_test() ->
    ProjectedColumns = [<<"r">>,<<"a">>,<<"i">>,<<"n">>],
    {Pfitting, Res, Rows} = process_setup(ProjectedColumns),
    ?assertNotEqual([], Rows),
    ExpectedRows = [],
    ExpectedErrors = [{invalid_column_mapping, <<"n">>}],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({error, riak_ql_pfitting_process_result:create(ProjectedColumns,
                                                                ExpectedRows,
                                                                ExpectedErrors)},
                 Processed).

-endif. %%TEST
