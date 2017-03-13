%% -------------------------------------------------------------------
%%
%% riak_ql_pfitting_arithmetic: Riak Query Pipeline pipe fitting (pfitting)
%% for arithmetic functions including: add, subtract, multiply, divide.
%% Multiple row result sets are processed to have a column containing the
%% result of the arithmetic function for each column mapping defined for the
%% pfitting.
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
-module(riak_ql_pfitting_arithmetic).
-behaviour(riak_ql_pfitting).

-define(QLPFMod, riak_ql_pfitting).
-define(QLPRMod, riak_ql_pfitting_process_result).
-define(QLCMMod, riak_ql_pfitting_column_mapping).

-export([process/3]).
-export([create_arithmetic_column_mapping/3]).

%% TODO: include SQL_NULL instead
-ifndef(SQL_NULL).
-define(SQL_NULL, []).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. %%TEST

-type column_identifier_or_constant() :: {identifier, [binary()]} |
                                         {constant, term()}.

-spec create_arithmetic_column_mapping(atom(),
                                       ?QLCMMod:constant_or_identifier(),
                                       ?QLCMMod:constant_or_identifier()) ->
    ?QLCMMod:column_mapping().
create_arithmetic_column_mapping(ArithmeticFunA, LHS, RHS) ->
    DisplayText = column_as_arithmetic(ArithmeticFunA, LHS, RHS),
    ColumnIdentifier = ?QLCMMod:set_column_identifier(DisplayText),
    OutputType = ?QLCMMod:set_resolvable_field_type(unresolved),
    MappingFun = ?QLCMMod:set_column_mapping_fun(
                    arithmetic_fun_atom_to_fun(ArithmeticFunA)),
    MappingFunArgs = ?QLCMMod:set_column_mapping_fun_args([LHS, RHS]),
    ?QLCMMod:create(ColumnIdentifier, DisplayText,
                    OutputType,
                    MappingFun,
                    MappingFunArgs).

arithmetic_fun_atom_to_fun(add) ->
    fun arithmetic_fun_add/2;
arithmetic_fun_atom_to_fun(subtract) ->
    fun arithmetic_fun_subtract/2;
arithmetic_fun_atom_to_fun(multiply) ->
    fun arithmetic_fun_multiply/2;
arithmetic_fun_atom_to_fun(divide) ->
    fun arithmetic_fun_divide/2.

constant_as_binary(C) ->
    [C1] = io_lib:format("~p", [C]),
    list_to_binary(C1).

-spec column_identifier_or_constant(?QLCMMod:constant_or_identifier()) ->
    column_identifier_or_constant().
column_identifier_or_constant(C) ->
    Type = ?QLCMMod:get_constant_or_identifier_type(C),
    column_identifier_or_constant1(Type).
column_identifier_or_constant1({identifier, ColumnIdentifier}) ->
    {identifier, [_Column]} = ?QLCMMod:get_column_identifier(ColumnIdentifier);
column_identifier_or_constant1({constant, Constant}) ->
    {constant, _C} = ?QLCMMod:get_constant(Constant).

-spec column_as_arithmetic(atom(), ?QLCMMod:constant_or_identifier(),
                           ?QLCMMod:constant_or_identifier()) ->
    binary().
column_as_arithmetic(AFunL, LHS, RHS) ->
    LHS1 = column_identifier_or_constant(LHS),
    RHS1 = column_identifier_or_constant(RHS),
    column_as_arithmetic1(AFunL, LHS1, RHS1).

-spec column_as_arithmetic1(atom(),
                            column_identifier_or_constant(),
                            column_identifier_or_constant()) ->
    binary().
column_as_arithmetic1(add, LHS, RHS) ->
    column_as_arithmetic2(<<" + ">>, LHS, RHS);
column_as_arithmetic1(subtract, LHS, RHS) ->
    column_as_arithmetic2(<<" - ">>, LHS, RHS);
column_as_arithmetic1(multiply, LHS, RHS) ->
    column_as_arithmetic2(<<" * ">>, LHS, RHS);
column_as_arithmetic1(divide, LHS, RHS) ->
    column_as_arithmetic2(<<" / ">>, LHS, RHS).

-spec column_as_arithmetic2(binary(),
                            column_identifier_or_constant(),
                            column_identifier_or_constant()) ->
    binary().
column_as_arithmetic2(AFunL, {identifier, [IdentifierL]}, {identifier, [IdentifierR]}) ->
    <<IdentifierL/binary, AFunL/binary, IdentifierR/binary>>;
column_as_arithmetic2(AFunL, {identifier, [IdentifierL]}, {constant, ConstantR}) ->
    CR = constant_as_binary(ConstantR),
    <<IdentifierL/binary, AFunL/binary, CR/binary>>;
column_as_arithmetic2(AFunL, {constant, ConstantL}, {identifier, [IdentifierR]}) ->
    CL = constant_as_binary(ConstantL),
    <<CL/binary, AFunL/binary, IdentifierR/binary>>;
column_as_arithmetic2(AFunL, {constant, ConstantL}, {constant, ConstantR}) ->
    CR = constant_as_binary(ConstantR),
    CL = constant_as_binary(ConstantL),
    <<CL/binary, AFunL/binary, CR/binary>>.

process(Pfitting, Columns, Rows) ->
    ColumnMappings = ?QLPFMod:get_column_mappings(Pfitting),
    Res = process_arithmetic(Columns, Rows, ColumnMappings),
    ResStatus = ?QLPRMod:get_status(Res),
    {ResStatus, Res}.

arithmetic_fun(_F, ?SQL_NULL, ?SQL_NULL) ->
    ?SQL_NULL;
arithmetic_fun(_F, _LHS, ?SQL_NULL) ->
    ?SQL_NULL;
arithmetic_fun(_F, ?SQL_NULL, _RHS) ->
    ?SQL_NULL;
arithmetic_fun(F, LHS, RHS) ->
    F(LHS, RHS).

add(LHS, RHS) -> LHS + RHS.
arithmetic_fun_add(LHS, RHS) -> arithmetic_fun(fun add/2, LHS, RHS).

subtract(LHS, RHS) -> LHS - RHS.
arithmetic_fun_subtract(LHS, RHS) -> arithmetic_fun(fun subtract/2, LHS, RHS).

multiply(LHS, RHS) -> LHS * RHS.
arithmetic_fun_multiply(LHS, RHS) -> arithmetic_fun(fun multiply/2, LHS, RHS).

divide(LHS, RHS) -> LHS / RHS.
arithmetic_fun_divide(LHS, RHS) -> arithmetic_fun(fun divide/2, LHS, RHS).

deref_value({constant, C}, _Row, _Columns) ->
    C;
deref_value({identifier, [_I]}, [], []) ->
    ?SQL_NULL;
deref_value({identifier, [I]}, [RH|_RT], [CH|_CT]) when CH =:= I ->
    RH;
deref_value(V={identifier, [_I]}, [_RH|RT], [_CH|CT]) ->
    deref_value(V, RT, CT).

map_row(Row, Columns, ColumnMappings) ->
    map_row_(Row, Columns, ColumnMappings, []).
map_row_(_Row, _Columns, [], Agg) ->
    lists:reverse(Agg);
map_row_(Row, Columns, [{ArithmeticFun, [LHS, RHS]}|ColumnMappingT], Agg) ->
    LHSV = deref_value(LHS, Row, Columns),
    RHSV = deref_value(RHS, Row, Columns),
    V = arithmetic_fun(ArithmeticFun, LHSV, RHSV),
    map_row_(Row, Columns, ColumnMappingT, [V|Agg]).

map_fun_args(A) ->
    map_fun_args1(?QLCMMod:get_column_mapping_fun_args(A)).
map_fun_args1(A) ->
    [?QLCMMod:get_constant_or_identifier(C) || C <- A].

process_arithmetic(Columns, Rows, ColumnMappings) ->
    ArithmeticColumns = [ ?QLCMMod:get_display_text(ColumnMapping) ||
                     ColumnMapping <- ColumnMappings ],
    ColumnMappings1 = [{?QLCMMod:get_fun(ColumnMapping),
                        map_fun_args(?QLCMMod:get_fun_args(ColumnMapping))
                       } || ColumnMapping <- ColumnMappings],
    {ProcessRows, ProcessErrors} =
    try [map_row(Row, Columns, ColumnMappings1) || Row <- Rows] of
        V -> {V, []}
    catch
        error:Error -> {[], [Error]}
    end,
    ?QLPRMod:create(ArithmeticColumns, ProcessRows, ProcessErrors).

-ifdef(TEST).
column_as_arithmetic_identifiers_test() ->
    ?assertEqual(<<"ab + cd">>,
                 column_as_arithmetic(add,
                                       ?QLCMMod:set_constant_or_identifier(
                                          {identifier, [<<"ab">>]}),
                                       ?QLCMMod:set_constant_or_identifier(
                                          {identifier, [<<"cd">>]}))).

column_as_arithmetic_identifier_constant_test() ->
    ?assertEqual(<<"ab / 17">>,
                 column_as_arithmetic(divide,
                                       ?QLCMMod:set_constant_or_identifier(
                                          {identifier, [<<"ab">>]}),
                                       ?QLCMMod:set_constant_or_identifier(
                                          {constant, 17}))).

column_as_arithmetic_constant_identifier_test() ->
    ?assertEqual(<<"13.0 * cd">>,
                 column_as_arithmetic(multiply,
                                       ?QLCMMod:set_constant_or_identifier(
                                          {constant, 13.0}),
                                       ?QLCMMod:set_constant_or_identifier(
                                          {identifier, [<<"cd">>]}))).

column_as_arithmetic_constants_test() ->
    ?assertEqual(<<"1000 - 27.0">>,
                 column_as_arithmetic(subtract,
                                       ?QLCMMod:set_constant_or_identifier(
                                          {constant, 1000}),
                                       ?QLCMMod:set_constant_or_identifier(
                                          {constant, 27.0}))).

map_simple_arithmetic_column_mappings(SimpleColumnMappings) ->
    [create_arithmetic_column_mapping(ArithmeticFunA, LHS, RHS) ||
     {ArithmeticFunA, LHS, RHS} <- SimpleColumnMappings].

process_setup(SimpleColumnMappings) ->
    ColumnMappings = map_simple_arithmetic_column_mappings(SimpleColumnMappings),
    Pfitting = ?QLPFMod:create(?MODULE, ColumnMappings),
    Columns = [<<"r">>, <<"i">>, <<"a">>, <<"k">>],
    Rows = [[1, <<"one">>, 1000, 1.0],
            [[],[],[],[]],
            [[], <<"two">>, 2000, 2.0],
            [3, [], 3000, 3.0],
            [4, <<"four">>, [], 4.0],
            [5, <<"five">>, 5000, []]],
    {Pfitting, Columns, Rows}.

expected_rows_single_identifier(ArithmeticFun, FieldExtractorFun, Constant, Rows) ->
    [[ArithmeticFun(FieldExtractorFun(Row), Constant)] ||
     Row <- Rows].

expected_rows_two_identifiers(ArithmeticFun, FieldExtractorFun, Rows) ->
    AFun = fun([LHS, RHS]) -> ArithmeticFun(LHS, RHS) end,
    [[AFun(FieldExtractorFun(Row))] ||
     Row <- Rows].

process_add_third_field_and_constant_test() ->
    ArithmeticFunA = add,
    ArithmeticFun = arithmetic_fun_atom_to_fun(ArithmeticFunA),
    LHS = ?QLCMMod:set_constant_or_identifier({identifier, [<<"a">>]}),
    Constant = 50,
    RHS = ?QLCMMod:set_constant_or_identifier({constant, Constant}),
    ExpectedColumns = [ column_as_arithmetic(ArithmeticFunA, LHS, RHS) ],
    {Pfitting, Columns, Rows} = process_setup([{ArithmeticFunA, LHS, RHS}]),
    ExpectedRows = expected_rows_single_identifier(ArithmeticFun,
                                                   fun ([_F1, _F2, F3, _F4]) -> F3 end,
                                                   Constant, Rows),
    ExpectedErrors = [],
    Processed = ?MODULE:process(Pfitting, Columns, Rows),
    ?assertEqual({ok, ?QLPRMod:create(ExpectedColumns,
                                      ExpectedRows,
                                      ExpectedErrors)},
                 Processed).

process_subtract_constant_and_constant_test() ->
    ArithmeticFunA = subtract,
    ConstantL = 73,
    LHS = ?QLCMMod:set_constant_or_identifier({constant, ConstantL}),
    ConstantR = 51,
    RHS = ?QLCMMod:set_constant_or_identifier({constant, ConstantR}),
    ExpectedColumns = [ column_as_arithmetic(ArithmeticFunA, LHS, RHS) ],
    {Pfitting, Columns, Rows} = process_setup([{ArithmeticFunA, LHS, RHS}]),
    ExpectedRows = [[ConstantL - ConstantR] || _Row <- Rows],
    ExpectedErrors = [],
    Processed = ?MODULE:process(Pfitting, Columns, Rows),
    ?assertEqual({ok, ?QLPRMod:create(ExpectedColumns,
                                      ExpectedRows,
                                      ExpectedErrors)},
                 Processed).

process_divide_two_fields_test() ->
    ArithmeticFunA = divide,
    ArithmeticFun = arithmetic_fun_atom_to_fun(ArithmeticFunA),
    LHS = ?QLCMMod:set_constant_or_identifier({identifier, [<<"a">>]}),
    RHS = ?QLCMMod:set_constant_or_identifier({identifier, [<<"k">>]}),
    ExpectedColumns = [ column_as_arithmetic(ArithmeticFunA, LHS, RHS) ],
    {Pfitting, Columns, Rows} = process_setup([{ArithmeticFunA, LHS, RHS}]),
    ExpectedRows = expected_rows_two_identifiers(ArithmeticFun,
                                                   fun ([_F1, _F2, F3, F4]) -> [F3, F4] end,
                                                   Rows),
    ExpectedErrors = [],
    Processed = ?MODULE:process(Pfitting, Columns, Rows),
    ?assertEqual({ok, ?QLPRMod:create(ExpectedColumns,
                                      ExpectedRows,
                                      ExpectedErrors)},
                 Processed).

process_multiply_badarith_test() ->
    ArithmeticFunA = multiply,
    LHS = ?QLCMMod:set_constant_or_identifier({constant, 3}),
    RHS = ?QLCMMod:set_constant_or_identifier({identifier, [<<"i">>]}),
    ExpectedColumns = [ column_as_arithmetic(ArithmeticFunA, LHS, RHS) ],
    {Pfitting, Columns, Rows} = process_setup([{ArithmeticFunA, LHS, RHS}]),
    ExpectedRows = [],
    ExpectedErrors = [badarith],
    Processed = ?MODULE:process(Pfitting, Columns, Rows),
    ?assertEqual({error, ?QLPRMod:create(ExpectedColumns,
                                         ExpectedRows,
                                         ExpectedErrors)},
                 Processed).

-endif. %%TEST
