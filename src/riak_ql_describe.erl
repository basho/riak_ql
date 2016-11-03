%%-------------------------------------------------------------------
%%
%% riak_ql_describe
%%
%% Copyright (C) 2016 Basho Technologies, Inc. All rights reserved
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
%%-------------------------------------------------------------------

-module(riak_ql_describe).

-export([describe/1]).

-include("riak_ql_ddl.hrl").

%%
-spec describe(?DDL{}) -> {ok, {ColNames::[binary()],
                                ColTypes::[riak_ql_ddl:simple_field_type()],
                                Rows::[[any()]]}}.
describe(?DDL{fields = FieldSpecs,
              partition_key = #key_v1{ast = PKSpec},
              local_key     = #key_v1{ast = LKSpec}}) ->
    ColumnNames = [<<"Column">>, <<"Type">>, <<"Nullable">>, <<"Partition Key">>, <<"Local Key">>, <<"Interval">>, <<"Unit">>, <<"Sort Order">>],
    ColumnTypes = [   varchar,      varchar,    boolean,       sint64,            sint64,         sint64,         varchar,      varchar],
    Quantum = find_quantum_field(PKSpec),
    Rows =
        [[Name,
          atom_to_binary(Type, latin1),
          Nullable,
          column_pk_position(Name, PKSpec),
          column_lk_position(Name, LKSpec),
          column_quantum_interval(Name, Quantum),
          column_quantum_unit(Name, Quantum),
          column_lk_order(Name, LKSpec)
         ]
         || #riak_field_v1{name = Name,
                           type = Type,
                           optional = Nullable} <- FieldSpecs],
    {ok, {ColumnNames, ColumnTypes, Rows}}.

%% Return the sort order of the local key for this column, or null if it is not
%% a local key or has an undefined sort order.
column_lk_order(Name, LK) when is_binary(Name) ->
    case lists:keyfind([Name], #riak_field_v1.name, LK) of
        ?SQL_PARAM{ordering = descending} ->
            <<"DESC">>;
        ?SQL_PARAM{ordering = ascending} ->
            <<"ASC">>;
        _ ->
            ?SQL_NULL
    end.

%% the following two functions are identical, for the way fields and
%% keys are represented as of 2015-12-18; duplication here is a hint
%% of things to come.
-spec column_pk_position(binary(), [?SQL_PARAM{}]) -> integer() | [].
column_pk_position(Col, KSpec) ->
    find_column_index(Col, KSpec, 1).

-spec column_lk_position(binary(), [?SQL_PARAM{}]) -> integer() | [].
column_lk_position(Col, KSpec) ->
    find_column_index(Col, KSpec, 1).

%% Extract the quantum column information, if it exists in the table definition
%% and put in two additional columns
-spec column_quantum_interval(Col :: binary(), PKSpec::#hash_fn_v1{}|[]) ->
      integer()|[].
column_quantum_interval(Col, #hash_fn_v1{args = [?SQL_PARAM{name = [Col]}, Interval, _]}) ->
    Interval;
column_quantum_interval(_, _) ->
    ?SQL_NULL.

-spec column_quantum_unit(Col::binary(), PKSpec::#hash_fn_v1{}|[]) ->
      binary()|[].
column_quantum_unit(Col, #hash_fn_v1{args = [?SQL_PARAM{name = [Col]}, _, Unit]}) ->
    atom_to_binary(Unit, latin1);
column_quantum_unit(_, _) ->
    ?SQL_NULL.

%% Find the field associated with the quantum, if there is one
-spec find_quantum_field([?SQL_PARAM{}|#hash_fn_v1{}]) -> [] | #hash_fn_v1{}.
find_quantum_field([]) ->
    ?SQL_NULL;
find_quantum_field([Q = #hash_fn_v1{}|_]) ->
    Q;
find_quantum_field([_|T]) ->
    find_quantum_field(T).

find_column_index(_, [], _) ->
    ?SQL_NULL;
find_column_index(Col, [?SQL_PARAM{name = [Col]} | _], Pos) ->
    Pos;
find_column_index(Col, [#hash_fn_v1{args = [?SQL_PARAM{name = [Col]} | _]} | _], Pos) ->
    Pos;
find_column_index(Col, [_ | Rest], Pos) ->
    find_column_index(Col, Rest, Pos + 1).

%%-------------------------------------------------------------------
%% Unit tests
%%-------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

assert_column_values(ColName, Expected, {Cols, _, Rows}) when is_binary(ColName),
                                                              is_list(Expected) ->
    Index = (catch lists:foldl(
        fun(E, Acc) when E == ColName ->
            throw(Acc);
           (_, Acc) ->
            Acc + 1
        end, 1, Cols)),
    % ?debugFmt("INDEX ~p COLS ~p~nROWS ~p", [Index, Cols, Rows]),
    Actual = [lists:nth(Index,R) || R <- Rows],
    ?assertEqual(
        Expected,
        Actual
    ).

names_types_and_rows_are_same_length_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
          riak_ql_lexer:get_tokens(
            "CREATE TABLE tab ("
            "a varchar   NOT NULL,"
            "b varchar   NOT NULL,"
            "c timestamp NOT NULL,"
            "PRIMARY KEY ((a, b, quantum(c, 15, m)), a, b, c))")),
    {ok, {Names, Types, [Row|_]}} = describe(DDL),
    ?assertEqual(length(Names), length(Types)),
    ?assertEqual(length(Names), length(Row)).

describe_table_column_names_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
          riak_ql_lexer:get_tokens(
            "CREATE TABLE tab ("
            "a VARCHAR   NOT NULL,"
            "b VARCHAR   NOT NULL,"
            "c TIMESTAMP NOT NULL,"
            "PRIMARY KEY ((a, b, quantum(c, 15, m)), a, b, c))")),
    {ok, Result} = describe(DDL),
    assert_column_values(
        <<"Column">>,
        [<<"a">>, <<"b">>, <<"c">>],
        Result
    ).

describe_nullable_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
            riak_ql_lexer:get_tokens(
                "CREATE TABLE tab ("
                " f VARCHAR   NOT NULL,"
                " s VARCHAR   NOT NULL,"
                " t TIMESTAMP NOT NULL,"
                " w SINT64,"
                " p DOUBLE,"
                " PRIMARY KEY ((f, s, t), f, s, t))")),
    {ok, Result} = describe(DDL),
    assert_column_values(<<"Nullable">>, [false,false,false,true,true], Result).

describe_table_quantum_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
          riak_ql_lexer:get_tokens(
            "CREATE TABLE tab ("
            "a VARCHAR   NOT NULL,"
            "b VARCHAR   NOT NULL,"
            "c TIMESTAMP NOT NULL,"
            "PRIMARY KEY ((a, b, quantum(c, 15, m)), a, b, c DESC))")),
    {ok, Result} = describe(DDL),
    assert_column_values(<<"Interval">>, [[], [], 15], Result),
    assert_column_values(<<"Unit">>,     [[], [], <<"m">>], Result).

describe_table_column_types_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
          riak_ql_lexer:get_tokens(
            "CREATE TABLE tab ("
            "a VARCHAR   NOT NULL,"
            "b SINT64    NOT NULL,"
            "c TIMESTAMP NOT NULL,"
            "d DOUBLE    NOT NULL,"
            "e BOOLEAN   NOT NULL,"
            "PRIMARY KEY ((a), a))")),
    {ok, Result} = describe(DDL),
    assert_column_values(
        <<"Type">>,
        [<<"varchar">>, <<"sint64">>, <<"timestamp">>, <<"double">>, <<"boolean">>],
        Result
    ).

describe_table_columns_no_quantum_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
            riak_ql_lexer:get_tokens(
                "CREATE TABLE fafa ("
                " f VARCHAR   NOT NULL,"
                " s VARCHAR   NOT NULL,"
                " t TIMESTAMP NOT NULL,"
                " w SINT64    NOT NULL,"
                " p DOUBLE,"
                " PRIMARY KEY ((f, s, t), f, s, t))")),
    {ok, Result} = describe(DDL),
    NullRow = [[],[],[],[],[]],
    assert_column_values(<<"Interval">>, NullRow, Result),
    assert_column_values(<<"Unit">>, NullRow, Result).

describe_table_descending_keys_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
          riak_ql_lexer:get_tokens(
            "CREATE TABLE tab ("
            "a VARCHAR   NOT NULL,"
            "b VARCHAR   NOT NULL,"
            "c TIMESTAMP NOT NULL,"
            "PRIMARY KEY ((a, b, quantum(c, 15, m)), a, b, c DESC))")),
    {ok, Result} = describe(DDL),
    assert_column_values(
        <<"Sort Order">>,
        [[], [], <<"DESC">>],
        Result
    ).

describe_table_ascending_keys_test() ->
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
          riak_ql_lexer:get_tokens(
            "CREATE TABLE tab ("
            "a VARCHAR   NOT NULL,"
            "b VARCHAR   NOT NULL,"
            "c TIMESTAMP NOT NULL,"
            "PRIMARY KEY ((a, b, quantum(c, 15, m)), a, b ASC, c ASC))")),
    {ok, Result} = describe(DDL),
    assert_column_values(
        <<"Sort Order">>,
        [[], <<"ASC">>, <<"ASC">>],
        Result
    ).

-endif.
