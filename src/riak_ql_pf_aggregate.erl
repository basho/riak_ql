%% -------------------------------------------------------------------
%%
%% riak_ql_pf_aggregate: Riak Query Pipeline pipe fitting (pfitting)
%% for aggregate functions
%%
%% @doc Riak Query Pipeline fitting (pfitting) including for aggregate: count,
%% sum, min, max. Multiple row result sets are processed down to a single row
%% with a column containing the result of the aggregate function for each column
%% defined for the pfitting.
%%
%% Copyright (c) 2016-2017 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_ql_pf_aggregate).
-behaviour(riak_ql_pfitting).

-export([process/3]).
-export([create_aggregate_column_mapping/2,
         create_aggregate_column_mapping/3]).

%% TODO: include SQL_NULL instead
-ifndef(SQL_NULL).
-define(SQL_NULL, []).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. %%TEST

-type aggregation_op() :: sum|count|min|max.
-export_type([aggregation_op/0]).

-spec create_aggregate_column_mapping(aggregation_op(),
                                      riak_ql_pf_mapping:column_identifier()) ->
    riak_ql_pf_mapping:column_mapping().
create_aggregate_column_mapping(AggregationOp, ColumnIdentifier) ->
    create_aggregate_column_mapping(AggregationOp,
                                    riak_ql_pf_mapping:set_column_mapping_fun(
                                       aggregate_fun_atom_to_fun(AggregationOp)),
                                    ColumnIdentifier).
-spec create_aggregate_column_mapping(aggregation_op(),
                                      riak_ql_pf_mapping:column_mapping_function(),
                                      riak_ql_pf_mapping:column_identifier()) ->
    riak_ql_pf_mapping:column_mapping().
create_aggregate_column_mapping(AggregationOp, AggregateFun, ColumnIdentifier) ->
    {identifier, [Column]} = riak_ql_pf_mapping:get_column_ident(ColumnIdentifier),
    OutputDisplayText = column_as_aggregate(AggregationOp, Column),
    OutputType = riak_ql_pf_mapping:set_resolvable_field_type(unresolved),
    AggregateFunArgs = riak_ql_pf_mapping:empty_args(),
    riak_ql_pf_mapping:create(ColumnIdentifier,
                              OutputDisplayText,
                              OutputType,
                              AggregateFun, AggregateFunArgs).

process(Pfitting, Columns, Rows) ->
    ColumnMappings = riak_ql_pfitting:get_column_mappings(Pfitting),
    Res = process_aggregate(Columns, Rows, ColumnMappings),
    ResStatus = riak_ql_pf_result:get_status(Res),
    {ResStatus, Res}.

-spec aggregate_fun_atom_to_fun(aggregation_op()) -> function().
aggregate_fun_atom_to_fun(sum) ->
    fun agg_fun_sum/2;
aggregate_fun_atom_to_fun(count) ->
    fun agg_fun_count/2;
aggregate_fun_atom_to_fun(min) ->
    fun agg_fun_min/2;
aggregate_fun_atom_to_fun(max) ->
    fun agg_fun_max/2.

-spec column_as_aggregate(aggregation_op(), binary()) -> binary().
column_as_aggregate(sum, Column) when is_binary(Column) ->
    list_to_binary("SUM(" ++ binary_to_list(Column) ++ ")");
column_as_aggregate(count, Column) when is_binary(Column) ->
    list_to_binary("COUNT(" ++ binary_to_list(Column) ++ ")");
column_as_aggregate(min, Column) when is_binary(Column) ->
    list_to_binary("MIN(" ++ binary_to_list(Column) ++ ")");
column_as_aggregate(max, Column) when is_binary(Column) ->
    list_to_binary("MAX(" ++ binary_to_list(Column) ++ ")").

agg_fun_sum(?SQL_NULL, Acc) ->
    Acc;
agg_fun_sum(FValue, ?SQL_NULL) ->
    FValue;
agg_fun_sum(FValue, Acc) ->
    Acc + FValue.

agg_fun_count(?SQL_NULL, Acc) ->
    Acc;
agg_fun_count(_FValue, ?SQL_NULL) ->
    1;
agg_fun_count(_FValue, Acc) ->
    Acc + 1.

agg_fun_min(?SQL_NULL, Acc) ->
    Acc;
agg_fun_min(FValue, ?SQL_NULL) ->
    FValue;
agg_fun_min(FValue, Acc) when FValue < Acc ->
    FValue;
agg_fun_min(_FValue, Acc) ->
    Acc.

agg_fun_max(?SQL_NULL, Acc) ->
    Acc;
agg_fun_max(FValue, ?SQL_NULL) ->
    FValue;
agg_fun_max(FValue, Acc) when FValue > Acc ->
    FValue;
agg_fun_max(_FValue, Acc) ->
    Acc.

row_aggregate(_Row, _Columns, [], _ORow, _OColumns, Acc) ->
    Acc; %%<< do NOT reverse, this is a proplist
row_aggregate([RowV|_RowT], [Column|_ColumnT],
              [{Column, ColumnMappingFun}|ColumnMappingT],
              ORow, OColumns, Acc) ->
    CAccId = {Column, ColumnMappingFun},
    CAcc = proplists:get_value(CAccId, Acc, ?SQL_NULL),
    AValue = ColumnMappingFun(RowV, CAcc),
    Acc1 = lists:keystore(CAccId, 1, Acc, {CAccId, AValue}),
    row_aggregate(ORow, OColumns, ColumnMappingT,
                  ORow, OColumns, Acc1);
row_aggregate([_RowV|RowT], [_Column|ColumnT], ColumnMappings,
              ORow, OColumns, Acc) ->
    row_aggregate(RowT, ColumnT, ColumnMappings,
                  ORow, OColumns, Acc).

process_aggregate(Columns, Rows, ColumnMappings) ->
    AggregateColumns = [ riak_ql_pf_mapping:get_display_text(ColumnMapping) ||
                         ColumnMapping <- ColumnMappings ],
    ColumnIdentifiers = [riak_ql_pf_mapping:get_column_ident(
                            riak_ql_pf_mapping:get_const_or_ident(
                               {identifier, [Column]})) ||
                         Column <- Columns ],
    ColumnMappings1 = [ {riak_ql_pf_mapping:get_column_ident(
                            riak_ql_pf_mapping:get_input_identifier(ColumnMapping)),
                         riak_ql_pf_mapping:get_column_mapping_fun(
                            riak_ql_pf_mapping:get_fun(ColumnMapping))}||
                        ColumnMapping <- ColumnMappings],
    { AggregateValues, ProcessErrors} =
    try lists:foldl(fun (Row, Acc) ->
                            row_aggregate(Row, ColumnIdentifiers, ColumnMappings1,
                                          Row, ColumnIdentifiers, Acc)
                    end, [], Rows) of
        V -> {V, []}
    catch
        error:Error -> {[], [Error]}
    end,
    AggregateRows = case length(ProcessErrors) of
                        0 -> [[V || {_K, V} <- AggregateValues]];
                        _ -> []
                    end,
    riak_ql_pf_result:create(AggregateColumns, AggregateRows, ProcessErrors).

-ifdef(TEST).

lex_parse(Sql) ->
    riak_ql_parser:parse(riak_ql_lexer:get_tokens(Sql)).

map_simple_aggregate_column_mappings(SimpleColumnMappings) ->
    [create_aggregate_column_mapping(AggregateFunA,
                                     riak_ql_pf_mapping:set_column_ident(Column)) ||
     {AggregateFunA, Column} <- SimpleColumnMappings].

process_setup(SimpleColumnMappings) ->
    ColumnMappings = map_simple_aggregate_column_mappings(SimpleColumnMappings),
    Pfitting = riak_ql_pfitting:create(?MODULE, ColumnMappings),
    {ok, Ast } = lex_parse("select r, i, a, k, ts from mock where ts > 0 and ts < 10"),
    Pspout = riak_ql_pspout_mock:create(Ast),
    Res = riak_ql_pspout:open(Pspout),
    Rows = riak_ql_pf_result:get_rows(Res),
    {Pfitting, Res, Rows}.

expected_row(FieldExtractorFun, AggregateFun, InitialAcc, Rows) ->
    [ lists:foldl(fun (Row, Acc) ->
                          FValue = FieldExtractorFun(Row),
                          AggregateFun(FValue, Acc)
                  end, InitialAcc, Rows) ].

process_sum_integer_field_test() ->
    AggregateColumn = <<"i">>,
    AggregateFunA = sum,
    AggregateFun = aggregate_fun_atom_to_fun(AggregateFunA),
    ExpectedColumns = [ column_as_aggregate(AggregateFunA, AggregateColumn) ],
    {Pfitting, Res, Rows } = process_setup([{AggregateFunA, AggregateColumn}]),
    ExpectedRows = [expected_row(fun ([_F1, F2, _F3, _F4, _F5]) -> F2 end, AggregateFun, ?SQL_NULL, Rows)],
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pf_result:create(ExpectedColumns,
                                      ExpectedRows,
                                      ExpectedErrors)},
                 Processed).

process_max_last_field_test() ->
    AggregateColumn = <<"k">>,
    AggregateFunA = max,
    AggregateFun = aggregate_fun_atom_to_fun(AggregateFunA),
    ExpectedColumns = [ column_as_aggregate(AggregateFunA, AggregateColumn) ],
    {Pfitting, Res, Rows } = process_setup([{AggregateFunA, AggregateColumn}]),
    ExpectedRows = [expected_row(fun ([_F1, _F2, _F3, F4, _F5]) -> F4 end, AggregateFun, ?SQL_NULL, Rows)],
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pf_result:create(ExpectedColumns,
                                      ExpectedRows,
                                      ExpectedErrors)},
                 Processed).

process_min_second_field_test() ->
    AggregateColumn = <<"i">>,
    AggregateFunA = min,
    AggregateFun = aggregate_fun_atom_to_fun(AggregateFunA),
    ExpectedColumns = [ column_as_aggregate(AggregateFunA, AggregateColumn) ],
    {Pfitting, Res, Rows } = process_setup([{AggregateFunA, AggregateColumn}]),
    ExpectedRows = [expected_row(fun ([_F1, F2, _F3, _F4, _F5]) -> F2 end, AggregateFun, ?SQL_NULL, Rows)],
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pf_result:create(ExpectedColumns,
                                      ExpectedRows,
                                      ExpectedErrors)},
                 Processed).

process_sum_first_field_test() ->
    AggregateColumn = <<"r">>,
    AggregateFunA = sum,
    AggregateFun = aggregate_fun_atom_to_fun(AggregateFunA),
    ExpectedColumns = [ column_as_aggregate(AggregateFunA, AggregateColumn) ],
    {Pfitting, Res, Rows } = process_setup([{AggregateFunA, AggregateColumn}]),
    ExpectedRows = [expected_row(fun ([F1, _F2, _F3, _F4, _F5]) -> F1 end, AggregateFun, ?SQL_NULL, Rows)],
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pf_result:create(ExpectedColumns,
                                      ExpectedRows,
                                      ExpectedErrors)},
                 Processed).

process_count_first_field_test() ->
    AggregateColumn = <<"r">>,
    AggregateFunA = count,
    AggregateFun = aggregate_fun_atom_to_fun(AggregateFunA),
    ExpectedColumns = [ column_as_aggregate(AggregateFunA, AggregateColumn) ],
    {Pfitting, Res, Rows } = process_setup([{AggregateFunA, AggregateColumn}]),
    ExpectedRows = [expected_row(fun ([F1, _F2, _F3, _F4, _F5]) -> F1 end, AggregateFun, ?SQL_NULL, Rows)],
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pf_result:create(ExpectedColumns,
                                      ExpectedRows,
                                      ExpectedErrors)},
                 Processed).

process_sum_varchar_field_test() ->
    AggregateColumn = <<"a">>,
    AggregateFunA = sum,
    ExpectedColumns = [ column_as_aggregate(AggregateFunA, AggregateColumn) ],
    {Pfitting, Res, Rows } = process_setup([{AggregateFunA, AggregateColumn}]),
    ?assertNotEqual([], Rows),
    ExpectedRows = [],
    ExpectedErrors = [badarith],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({error, riak_ql_pf_result:create(ExpectedColumns,
                                         ExpectedRows,
                                         ExpectedErrors)},
                 Processed).

process_aggregate_all_fields_test() ->
    ColumnMappings = [{min, <<"r">>},
                      {sum, <<"i">>},
                      {count, <<"a">>},
                      {count, <<"k">>},
                      {max, <<"ts">>}],
    NullRow = [ ?SQL_NULL || _I <- lists:seq(1, 5) ],
    AggregateFun = fun([F1, F2, F3, F4, F5], [A1, A2, A3, A4, A5]) ->
                           [agg_fun_min(F1, A1),
                            agg_fun_sum(F2, A2),
                            agg_fun_count(F3, A3),
                            agg_fun_count(F4, A4),
                            agg_fun_max(F5, A5)]
                   end,
    ExpectedColumns = [ column_as_aggregate(AggregateFunA, AggregateColumn) ||
                        {AggregateFunA, AggregateColumn} <- ColumnMappings],
    {Pfitting, Res, Rows } = process_setup(ColumnMappings),
    ExpectedRows = expected_row(fun (Row) -> Row end, AggregateFun, NullRow, Rows),
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pf_result:create(ExpectedColumns,
                                      ExpectedRows,
                                      ExpectedErrors)},
                 Processed).

process_aggregate_mean_preparation_test() ->
    %% this test stresses the accumulator's need to identify an aggregate,
    %% including the aggregate function, not just the identifier.
    %% and this test builds towards a key goal of the Riak Query Pipeline, to
    %% support AVERAGE (mean) by gathering the vnode-local aggregates in a
    %% manner that can be gathered and further processed, summing the sums and
    %% counts and dividing the resultant sums by counts at the coordinator who
    %% initiated the scatter-gather. Otherwise said, a lazy AVERAGE that in
    %% transit commutes.
    ColumnMappings = [{sum, <<"i">>},
                      {count, <<"i">>},
                      {sum, <<"ts">>},
                      {count, <<"ts">>}],
    NullRow = [ ?SQL_NULL || _I <- lists:seq(1, 5) ],
    SumFun = fun([_F1, F2, _F3, _F4, F5], [A1, A2, A3, A4, A5]) ->
                     [A1,
                      agg_fun_sum(F2, A2),
                      A3,
                      A4,
                      agg_fun_sum(F5, A5)]
             end,
    CountFun = fun([_F1, F2, _F3, _F4, F5], [A1, A2, A3, A4, A5]) ->
                     [A1,
                      agg_fun_count(F2, A2),
                      A3,
                      A4,
                      agg_fun_count(F5, A5)]
             end,
    ExpectedColumns = [ column_as_aggregate(AggregateFunA, AggregateColumn) ||
                        {AggregateFunA, AggregateColumn} <- ColumnMappings],
    {Pfitting, Res, Rows } = process_setup(ColumnMappings),
    [[_A1, ASum2, _A3, _A4, ASum5]] = expected_row(fun (Row) -> Row end, SumFun, NullRow, Rows),
    [[_A1, ACount2, _A3, _A4, ACount5]] = expected_row(fun (Row) -> Row end, CountFun, NullRow, Rows),
    ExpectedRows = [[ASum2, ACount2, ASum5, ACount5]],
    ExpectedErrors = [],
    Processed = riak_ql_pfitting:process(Pfitting, Res),
    ?assertEqual({ok, riak_ql_pf_result:create(ExpectedColumns,
                                      ExpectedRows,
                                      ExpectedErrors)},
                 Processed).

-endif. %%TEST
