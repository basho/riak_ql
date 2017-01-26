%% -------------------------------------------------------------------
%%
%% riak_ql_ddl: API module for the DDL
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
-module(riak_ql_ddl).

-include("riak_ql_ddl.hrl").

-export([
         apply_ordering/2,
         convert/2,
         current_version/0,
         ddl_record_version/1,
         first_version/0,
         flip_binary/1,
         fold_where_tree/3,
         get_field_type/2,
         get_storage_type/1,
         get_legacy_type/2,
         get_minimum_capability/1,
         is_version_greater/2,
         make_module_name/1, make_module_name/2,
         mapfold_where_tree/3
        ]).

-type external_field_type()         :: varchar | sint64 | double | timestamp | boolean | blob.
-type internal_field_type()         :: varchar | sint64 | double | timestamp | boolean.

%% Relational operators allowed in a where clause.
-type relational_op() :: '=' | '!=' | '>' | '<' | '<=' | '>='.

%% NULL comparison operator (comparator) allowed in where clause, but limited to
%% SQL "<field> IS [NOT] NULL"
-type null_comparator() :: is_null | is_not_null.

-type selection_function() :: {{window_agg_fn, FunctionName::atom()}, [any()]}.
-type inverse_distrib_function() :: {{inverse_distrib_fn, FunctionName::atom()},
                                     [field_identifier()|data_value()]}.

%% lexer-emitted data types
-type data_value()       :: {integer, integer()}
                          | {float, float()}
                          | {boolean, boolean()}
                          | {binary, binary()}
                          | {blob, binary()}
                          | {null, []}.
-type field_identifier() :: {identifier, [binary()]}.
-type selection()  :: field_identifier()
                    | data_value()
                    | selection_function()
                    | inverse_distrib_function()
                    | {expr, selection()}
                    | {negate, selection()}
                    | {relational_op(), selection(), selection()}
                    | {null_comparator(), field_identifier()}.

-type insertion()  :: field_identifier().
-type filter()     :: term().

-type ddl() :: ?DDL{}.
-type any_ddl() :: #ddl_v1{} | ?DDL{}.

-export_type([
              any_ddl/0,
              data_value/0,
              ddl/0,
              ddl_version/0,
              field_identifier/0,
              filter/0,
              selection/0,
              selection_function/0,
              external_field_type/0,
              internal_field_type/0
             ]).

%% this type is a synonym of riak_pb_ts_codec:ldbvalue(), but it's
%% defined locally here in order to avoid depending on riak_pb.
-type bare_data_value() :: integer() | float() | boolean() | binary() | [].

%% a helper function for destructuring data objects
%% and testing the validity of field names
%% the generated helper functions cannot contain
%% record definitions because of the build cycle
%% so this function can be called out to to pick
%% apart the DDL records

-export([
         get_local_key/2, get_local_key/3,
         get_partition_key/2, get_partition_key/3,
         get_table/1,
         insert_sql_columns/3,
         is_insert_valid/3,
         is_query_valid/3,
         lk_to_pk/2, lk_to_pk/3,
         make_key/3,
         syntax_error_to_msg/1
        ]).
%%-export([get_return_types_and_col_names/2]).

-type query_syntax_error() ::
        {bucket_type_mismatch, DDL_bucket::binary(), Query_bucket::binary()} |
        {incompatible_type, Field::binary(), external_field_type(), atom()} |
        {incompatible_operator, Field::binary(), external_field_type(), relational_op()}  |
        {unexpected_where_field, Field::binary()} |
        {unexpected_insert_field, Field::binary()} |
        {unexpected_select_field, Field::binary()} |
        {unknown_column_type, term()} |
        {selections_cant_be_blank, []} |
        {insertions_cant_be_blank, []}.

-export_type([query_syntax_error/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% for other testing modules
-export([parsed_sql_to_query/1]).
-endif.

-define(CANBEBLANK,  true).
-define(CANTBEBLANK, false).

-spec make_module_name(Table::binary()) -> module().
%% @doc Generate a unique module name for Table at version 1. @see
%%      make_module_name/2.
make_module_name(Table) ->
    make_module_name(Table, riak_ql_ddl_compiler:get_compiler_version()).

-spec make_module_name(Table   :: binary(),
                       Version :: riak_ql_component:component_version()) ->
                       module().
%% @doc Generate a unique, but readable and recognizable, module name
%%      for Table at a certain Version, by 'escaping' non-ascii chars
%%      in Table a la C++.
make_module_name(Table1, CompilerVersion) when is_binary(Table1), is_integer(CompilerVersion) ->
    Table2 = << <<(maybe_mangle_char(C))/binary>> || <<C>> <= Table1>>,
    ModName = <<"riak_ql_table_", Table2/binary, "_", (integer_to_binary(CompilerVersion))/binary>>,
    binary_to_atom(ModName, latin1).

maybe_mangle_char(C) when (C >= $a andalso C =< $z);
                          (C >= $A andalso C =< $Z);
                          (C >= $0 andalso C =< $9);
                          (C == $_) ->
    <<C>>;
maybe_mangle_char(C) ->
    <<$%, (list_to_binary(integer_to_list(C)))/binary>>.

-spec get_table(?DDL{}) -> binary().
get_table(?DDL{table = T}) ->
    T.

-spec get_partition_key(?DDL{}, tuple(data_value()), module()) -> [data_value()].
get_partition_key(?DDL{partition_key = PK}, Obj, Mod)
  when is_tuple(Obj) ->
    #key_v1{ast = Params} = PK,
    _Key = build(Params, Obj, Mod, []).

-spec get_partition_key(?DDL{}, tuple(data_value())) -> [data_value()].
get_partition_key(?DDL{table = T}=DDL, Obj)
  when is_tuple(Obj) ->
    Mod = make_module_name(T),
    get_partition_key(DDL, Obj, Mod).

-spec get_local_key(?DDL{}, tuple(data_value()), module()) -> [data_value()].
get_local_key(?DDL{local_key = LK}, Obj, Mod)
  when is_tuple(Obj) ->
    #key_v1{ast = Params} = LK,
    _Key = build(Params, Obj, Mod, []).

-spec get_local_key(?DDL{}, tuple(data_value())) -> [data_value()].
get_local_key(?DDL{table = T}=DDL, Obj)
  when is_tuple(Obj) ->
    Mod = make_module_name(T),
    get_local_key(DDL, Obj, Mod).

-spec lk_to_pk([bare_data_value()], ?DDL{}) -> {ok, [bare_data_value()]} |
                                   {error, {bad_key_length, integer(), integer()}}.
lk_to_pk(LKVals, DDL = ?DDL{table = Table}) ->
    lk_to_pk(LKVals, make_module_name(Table), DDL).

-spec lk_to_pk([bare_data_value()], module(), ?DDL{}) -> {ok, [bare_data_value()]} |
                                             {error, {bad_key_length, integer(), integer()}}.
%% @doc Determine the partition key of the quantum where given local
%%      key resides, observing DESC qualifiers where appropriate.
lk_to_pk(LKVals, Mod, ?DDL{local_key = #key_v1{ast = LKAst},
                           partition_key = PK}) ->
    KeyFields = [F || ?SQL_PARAM{name = [F]} <- LKAst],
    case {length(LKVals), length(KeyFields)} of
        {_N, _N} ->
            LKPairs = lists:zip(KeyFields, LKVals),
            PKVals = [V || {_Type, V} <- make_key(Mod, PK, LKPairs)],
            {ok, PKVals};
        {Got, Need} ->
            {error, {bad_key_length, Got, Need}}
    end.

-spec make_key(atom(), #key_v1{} | none, [{binary(), bare_data_value()}]) ->
                      [{external_field_type(), bare_data_value()}].
make_key(_Mod, none, _Vals) ->
    [];
make_key(Mod, #key_v1{ast = AST}, Vals) when is_atom(Mod)  andalso
                                             is_list(Vals) ->
    mk_k(AST, Vals, Mod, []).

%% TODO there is a mismatch between how the fields in the where clause
%% and the fields in the DDL are mapped
mk_k([], _Vals, _Mod, Acc) ->
    lists:reverse(Acc);
mk_k([#hash_fn_v1{mod = Md,
                  fn   = Fn,
                  args = Args,
                  type = Ty} | T1], Vals, Mod, Acc) ->
    A2 = extract(Args, Vals, []),
    V  = erlang:apply(Md, Fn, A2),
    mk_k(T1, Vals, Mod, [{Ty, V} | Acc]);
mk_k([?SQL_PARAM{name = [Nm], ordering = Ordering} | T1], Vals, Mod, Acc) ->
    case lists:keyfind(Nm, 1, Vals) of
        {Nm, V} ->
            Ty = Mod:get_field_type([Nm]),
            mk_k(T1, Vals, Mod, [{Ty, apply_ordering(V, Ordering)} | Acc]);
        false ->
            {error, {missing_value, Nm, Vals}}
    end.

-spec extract(list(), [data_value()], [bare_data_value()]) -> bare_data_value().
extract([], _Vals, Acc) ->
    lists:reverse(Acc);
extract([?SQL_PARAM{name = [Nm], ordering = Ordering} | T], Vals, Acc) ->
    {Nm, Val} = lists:keyfind(Nm, 1, Vals),
    extract(T, Vals, [apply_ordering(Val, Ordering) | Acc]);
extract([Constant | T], Vals, Acc) ->
    extract(T, Vals, [Constant | Acc]).

-spec build([?SQL_PARAM{}], tuple(data_value()), module(), [data_value()]) -> [data_value()].
build([], _Obj, _Mod, A) ->
    lists:reverse(A);
build([?SQL_PARAM{name = Nm, ordering = Ordering} | T], Obj, Mod, A) ->
    Val = Mod:extract(Obj, Nm),
    Type = Mod:get_field_type(Nm),
    build(T, Obj, Mod, [{Type, apply_ordering(Val, Ordering)} | A]);
build([#hash_fn_v1{mod  = Md,
                   fn   = Fn,
                   args = Args,
                   type = Ty} | T], Obj, Mod, A) ->
    A2 = convert(Args, Obj, Mod, []),
    Val = erlang:apply(Md, Fn, A2),
    build(T, Obj, Mod, [{Ty, Val} | A]).

-spec convert([?SQL_PARAM{}], tuple(), atom(), [bare_data_value()]) -> bare_data_value().
convert([], _Obj, _Mod, Acc) ->
    lists:reverse(Acc);
convert([?SQL_PARAM{name = Nm} | T], Obj, Mod, Acc) ->
    Val = Mod:extract(Obj, Nm),
    convert(T, Obj, Mod, [Val | Acc]);
convert([Constant | T], Obj, Mod, Acc) ->
    convert(T, Obj, Mod, [Constant | Acc]).

-spec apply_ordering(bare_data_value(), ascending|descending) -> bare_data_value().
apply_ordering(Val, descending) when is_integer(Val) ->
    -Val;
apply_ordering(Val, descending) when is_binary(Val) ->
    flip_binary(Val);
apply_ordering(Val, _) -> % ascending or undefined
    Val.

%%
-spec flip_binary(binary()) -> binary().
flip_binary(V) when is_binary(V) ->
    << <<(bnot X):8>> || <<X>> <= V >>.

%% Convert an error emitted from the :is_query_valid/3 function
%% and convert it into a user-friendly, text message binary.
-spec syntax_error_to_msg(query_syntax_error()) ->
                                 Msg::binary().
syntax_error_to_msg(E) ->
    {Fmt, Args} = syntax_error_to_msg2(E),
    iolist_to_binary(io_lib:format(Fmt, Args)).

%%
syntax_error_to_msg2({type_check_failed, Fn, Arity, ExprTypes}) ->
    {"Function ~s/~p called with arguments of the wrong type ~p.",
      [unquote_fn(Fn), Arity, ExprTypes]};
syntax_error_to_msg2({fn_called_with_wrong_arity, Fn, Arity, NumArgs}) ->
    {"Function ~s/~p called with ~p arguments.", [unquote_fn(Fn), Arity, NumArgs]};
syntax_error_to_msg2({bucket_type_mismatch, B1, B2}) ->
    {"bucket_type_mismatch: DDL bucket type was ~s "
     "but query selected from bucket type ~s.", [B1, B2]};
syntax_error_to_msg2({incompatible_type, Field, Expected, Actual}) ->
    {"incompatible_type: field ~s with type ~p cannot be compared "
     "to type ~p in where clause.", [Field, Expected, Actual]};
syntax_error_to_msg2({incompatible_insert_type, Field, Expected, Actual}) ->
    {"incompatible_insert_type: field ~s with type ~p cannot be inserted "
    "to type ~p .", [Field, Actual, Expected]};
syntax_error_to_msg2({incompatible_insert_type}) ->
    {"incompatible_insert_type: bad type for inserted field"};
syntax_error_to_msg2({incompatible_operator, Field, ColType, Op}) ->
    {"incompatible_operator: field ~s with type ~p cannot use "
     "operator ~p in where clause.", [Field, ColType, Op]};
syntax_error_to_msg2({unexpected_where_field, Field}) ->
    {"unexpected_where_field: unexpected field ~s in where clause.",
     [Field]};
syntax_error_to_msg2({unexpected_select_field, Field}) ->
    {"unexpected_select_field: unexpected field ~s in select clause.",
     [Field]};
syntax_error_to_msg2({unexpected_orderby_field, Field}) ->
    {"unexpected_orderby_field: unknown column ~s in order-by clause.",
     [Field]};
syntax_error_to_msg2({unexpected_insert_field, Field}) ->
    {"unexpected_select_field: unexpected field ~s in insert clause.",
     [Field]};
syntax_error_to_msg2({subexpressions_not_supported, Field, Op}) ->
    {"subexpressions_not_supported: expressions in where clause operators"
     " (~s ~s ...) are not supported.",
     [Field, Op]};
syntax_error_to_msg2({unknown_column, Field}) ->
    {"Unknown column \"~s\".", [Field]};
syntax_error_to_msg2({unknown_column_type, Other}) ->
    {"Unexpected select column type ~p.", [Other]};
syntax_error_to_msg2({invalid_field_operation}) ->
    {"Comparing or otherwise operating on two fields is not supported", []};
syntax_error_to_msg2({argument_type_mismatch, Fn, Args}) ->
    {"Function '~s' called with arguments of the wrong type ~p.", [Fn, Args]};
syntax_error_to_msg2({operator_type_mismatch, Fn, Type1, Type2}) ->
    {"Operator '~s' called with mismatched types [~p vs ~p].", [Fn, Type1, Type2]}.

%% An atom with upper case chars gets printed as 'COUNT' so remove the
%% quotes to make the error message more reable.
unquote_fn(Fn) when is_atom(Fn) ->
    string:strip(atom_to_list(Fn), both, $').

-spec is_query_valid(module(), ?DDL{}, {term(), term(), term(), term()}) ->
                            true | {false, [query_syntax_error()]}.
is_query_valid(_, ?DDL{ table = T1 },
               {T2, _Select, _Where, _OrderBy}) when T1 /= T2 ->
    {false, [{bucket_type_mismatch, {T1, T2}}]};
is_query_valid(Mod, _, {_Table, Selection, Where, OrderBy}) ->
    ValidSelection = are_selections_valid(Mod, Selection, ?CANTBEBLANK),
    ValidOrderBy   = is_orderby_valid(Mod, OrderBy),
    ValidFilters   = are_filters_valid(Mod, Where),
    is_query_valid_result([ValidSelection, ValidFilters, ValidOrderBy]).

-spec is_insert_valid(module(), ?DDL{}, {term(), term(), term()}) ->
                      true | {false, [query_syntax_error()]}.
is_insert_valid(_, ?DDL{ table = T1 },
                {T2, _Fields, _Values}) when T1 /= T2 ->
    {false, [{bucket_type_mismatch, {T1, T2}}]};
is_insert_valid(Mod, _DDL, {_Table, Fields, Values}) ->
    ValidColumns = are_insert_columns_valid(Mod, Fields, ?CANTBEBLANK),
    case ValidColumns of
        true ->
            are_insert_types_valid(Mod, Fields, Values);
        _ ->
            ValidColumns
    end.

%%
is_query_valid_result(Res) ->
    lists:foldl(
      fun(true, true) ->
              true;
         (true, Acc) ->
              Acc;
         ({false, Errs}, true) ->
              {false, Errs};
         ({false, Errs1}, {false, Errs2}) ->
              {false, Errs2 ++ Errs1}
      end,
      true, Res).

-spec are_filters_valid(module(), [filter()]) -> true | {false, [query_syntax_error()]}.
are_filters_valid(Mod, Where) ->
    Errors = fold_where_tree(fun(_, Clause, Acc) ->
                                     is_filters_field_valid(Mod, Clause, Acc)
                             end, [], Where),
    case Errors of
        [] -> true;
        _  -> {false, Errors}
    end.

%% the terminal case of "a = 2"
is_filters_field_valid(Mod, {Op, Field, {RHS_type, RHS_Val}}, Acc1) ->
    case Mod:is_field_valid([Field]) of
        true  ->
            ExpectedType = Mod:get_field_type([Field]),
            Acc2 = case is_compatible_type(get_storage_type(ExpectedType), RHS_type, normalise(RHS_Val)) of
                true  -> Acc1;
                false -> [{incompatible_type, Field, ExpectedType, RHS_type} | Acc1]
            end,
            case is_compatible_operator(Op, get_storage_type(ExpectedType), RHS_type) of
                true  -> Acc2;
                false -> [{incompatible_operator, Field, ExpectedType, Op} | Acc2]
            end;
        false ->
            [{unexpected_where_field, Field} | Acc1]
    end;
%% the case where field is [not] null is being tested
is_filters_field_valid(Mod, {NullOp, {identifier, Field}}, Acc1) when NullOp =:= is_null orelse
                                                                            NullOp =:= is_not_null ->
    case Mod:is_field_valid([Field]) of
        true ->
            Acc1;
        false ->
            [{unexpected_where_field, Field} | Acc1]
    end;
%% the case where two fields are being operated on
is_filters_field_valid(_Mod, {_Op, _Field1, _Field2}, Acc1) when is_binary(_Field1), is_binary(_Field2) ->
    [{invalid_field_operation} | Acc1];
%% the case where RHS is an expression on its own (LHS must still be a valid field)
is_filters_field_valid(_Mod, {Op, Field, {_RHS_op, _RHS_lhs_bare_value, _RHS_rhs}}, Acc1) ->
    [{subexpressions_not_supported, Field, Op} | Acc1].
%% andreiz: The code below would check for type compatibility
%% between field and expression, if subexpressions were
%% supported. Currently (2015-12-03), the query rewrite code in
%% riak_kv_qry_compiler cannot deal with subexpressions.  Uncomment
%% and edit the following when it does.

%% case Mod:is_field_valid([Field]) of
%%     true  ->
%%         ExpectedType = Mod:get_field_type([Field]),
%%         %% the lexer happens to have no type attached to LHS, even
%%         %% when it's not a field but an rvalue; just assume it is
%%         %% the type of the field at the root of the expression
%%         RHS_lhs = maybe_assign_type(RHS_lhs_bare_value, ExpectedType),

%%         %% this is the case of "A = 3 + 2":
%%         %% * check that A is compatible with 3 and 2 on '='
%%         %% * check that A is compatible with 3 and 2 on '+'
%%         lists:append(
%%           [is_filters_field_valid(Mod, {Op,     Field, RHS_lhs}, []),
%%            is_filters_field_valid(Mod, {Op,     Field, RHS_rhs}, []),
%%            is_filters_field_valid(Mod, {RHS_op, Field, RHS_lhs}, []),
%%            is_filters_field_valid(Mod, {RHS_op, Field, RHS_rhs}, []) | Acc1]);
%%     false ->
%%         [{unexpected_where_field, Field} | Acc1]
%% end.
%%
%% maybe_assign_type({_Type, _Value} = AlreadyTyped, _AttributedType) ->
%%     AlreadyTyped;
%% maybe_assign_type(BareValue, FieldType) ->
%%     {lexer_type_of(FieldType), BareValue}.
%%
%% lexer_type_of(timestamp) -> integer;
%% lexer_type_of(boolean)   -> boolean;
%% lexer_type_of(sint64)    -> integer;
%% lexer_type_of(double)    -> float;
%% lexer_type_of(varchar)   -> binary.

normalise(Bin) when is_binary(Bin) ->
    string:to_lower(binary_to_list(Bin));
normalise(X) -> X.

%% Check if the column type and the value being compared
%% are comparable.
-spec is_compatible_type(ColType::atom(), WhereType::atom(), bare_data_value()) ->
                                boolean().
is_compatible_type(timestamp, integer, _)       -> true;
is_compatible_type(boolean,   boolean,  true)   -> true;
is_compatible_type(boolean,   boolean,  false)  -> true;
is_compatible_type(sint64,    integer, _)       -> true;
is_compatible_type(double,    float,   _)       -> true;
is_compatible_type(varchar,   binary,  _)       -> true;
is_compatible_type(_, _, _) -> false.

%% Check that the operation being performed in a where clause, for example
%% we cannot check if one binary is greated than another one in SQL.
-spec is_compatible_operator(OP::relational_op(),
                             ExpectedType::internal_field_type(),
                             RHS_type::atom()) -> boolean().
is_compatible_operator('=',  varchar, binary) -> true;
is_compatible_operator('!=', varchar, binary) -> true;
is_compatible_operator(_,    varchar, binary) -> false;
is_compatible_operator('=',  boolean, boolean)-> true;
is_compatible_operator('!=', boolean, boolean)-> true;
is_compatible_operator(_,    boolean, boolean)-> false;
is_compatible_operator(_,_,_)                 -> true.

-spec are_selections_valid(module(), [selection()], boolean()) ->
                                  true | {false, [query_syntax_error()]}.
are_selections_valid(_, [], ?CANTBEBLANK) ->
    {false, [{selections_cant_be_blank, []}]};
are_selections_valid(Mod, Selections, _) ->
    CheckFn =
        fun(E, Acc) ->
                is_selection_column_valid(Mod, E, Acc)
        end,
    case lists:foldl(CheckFn, [], Selections) of
        []     -> true;
        Errors -> {false, lists:reverse(Errors)}
    end.

%% Reported error types must be supported by the function syntax_error_to_msg2
is_selection_column_valid(Mod, {identifier, X}, Acc) ->
    case Mod:is_field_valid(X) of
        true  ->
            Acc;
        false ->
            [{unexpected_select_field, hd(X)} | Acc]
    end;
is_selection_column_valid(_, {{window_agg_fn, Fn}, Args}, Acc) ->
    ArgLen = length(Args),
    case riak_ql_window_agg_fns:fn_arity(Fn) == ArgLen of
        false ->
            [{fn_called_with_wrong_arity, Fn, 1, length(Args)} | Acc];
        true ->
            Acc
    end;
is_selection_column_valid(_, {{inverse_distrib_fn, Fn}, Args}, Acc) ->
    ArgLen = length(Args),
    case riak_ql_inverse_distrib_fns:fn_arity(Fn) == ArgLen of
        false ->
            [{fn_called_with_wrong_arity, Fn, 1, length(Args)} | Acc];
        true ->
            Acc
    end;
is_selection_column_valid(_, {Type, _}, Acc) when is_atom(Type) ->
    %% literal types, integer double etc.
    Acc;
is_selection_column_valid(_, {Op, _, _}, Acc) when is_atom(Op) ->
    %% arithmetic
    Acc;
is_selection_column_valid(_, Other, Acc) ->
    [{unknown_column_type, Other} | Acc].


is_orderby_valid(_Mod, undefined) ->
    true;
is_orderby_valid(Mod, QualifiedFields) ->
    CheckFn =
        fun({F, _DirQualifier, _NullsGroupQualifier}, Acc) ->
                is_orderby_column_valid(Mod, F, Acc)
        end,
    case lists:foldl(CheckFn, [], QualifiedFields) of
        []     -> true;
        Errors -> {false, lists:reverse(Errors)}
    end.

is_orderby_column_valid(Mod, X, Acc) ->
    case Mod:is_field_valid([X]) of
        true  ->
            Acc;
        false ->
            [{unexpected_orderby_field, X} | Acc]
    end.


%% Fold over the syntax tree for a where clause.
fold_where_tree(Fn, Acc, Where) when is_function(Fn) ->
    fold_where_tree2(root, Fn, Acc, Where).

fold_where_tree2(_, _, Acc, []) ->
    Acc;
fold_where_tree2(Conditional, Fn, Acc1, [Where]) ->
    fold_where_tree2(Conditional, Fn, Acc1, Where);
fold_where_tree2(_, Fn, Acc1, {Op, LHS, RHS}) when Op == and_; Op == or_ ->
    Acc2 = fold_where_tree2(Op, Fn, Acc1, LHS),
    fold_where_tree2(Op, Fn, Acc2, RHS);
fold_where_tree2(Conditional, Fn, Acc, Clause) ->
    Fn(Conditional, Clause, Acc).

%% Like lists:mapfoldl but specifically for where clause AST. The fun will be
%% called on each filter e.g. `Fun(Conditional, Filter, Acc)`. 
%% * Conditional is either `_and`, `_or` or `root`.
%% * Filter AST tuple
%% * Acc is the provided accumulator
%%
%% The fun should return a tuple like `{MappedFilter,Acc2}`. Special returns for
%% `MappedValue are `eliminate` which removes the filter and skip which skips
%% this filter and any more in the same branch. 
mapfold_where_tree(_, Acc1, []) ->
    {[], Acc1};
mapfold_where_tree(Fn, Acc1, Where) when is_function(Fn) ->
    case mapfold_where_tree2(root, Fn, Acc1, Where) of
        {eliminate, Acc2} ->
            {[], Acc2};
        {_,_} = Return ->
            Return
    end.

mapfold_where_tree2(_, _, Acc, []) ->
    Acc;
mapfold_where_tree2(Conditional, Fn, Acc1, [Where]) ->
    mapfold_where_tree2(Conditional, Fn, Acc1, Where);
mapfold_where_tree2(Conditional, Fn, Acc1, {Op, LHS, RHS}) when Op == and_; Op == or_ ->
    case Fn(Conditional, Op, Acc1) of
        {ok, Acc2} ->
            {LHS_result, Acc3} = mapfold_where_tree2(Op, Fn, Acc2, LHS),
            {RHS_result, Acc4} = mapfold_where_tree2(Op, Fn, Acc3, RHS),
            case {LHS_result, RHS_result} of
                {eliminate, eliminate} ->
                    {eliminate, Acc4};
                {eliminate, _} ->
                    {RHS_result, Acc4};
                {_, eliminate} ->
                    {LHS_result, Acc4};
                {_, _} ->
                    {{Op, LHS_result, RHS_result}, Acc4}
            end;
        {skip, Acc2} ->
            {{Op, LHS, RHS}, Acc2}
    end;
mapfold_where_tree2(Conditional, Fn, Acc, Clause) ->
    Fn(Conditional, Clause, Acc).

-spec are_insert_columns_valid(module(), [insertion()], boolean()) ->
    true | {false, [query_syntax_error()]}.
are_insert_columns_valid(_, [], ?CANTBEBLANK) ->
    {false, [{insertions_cant_be_blank, []}]};
are_insert_columns_valid(Mod, Columns, _) ->
    CheckFn =
        fun(E, Acc) ->
            is_insert_column_valid(Mod, E, Acc)
        end,
    case lists:foldl(CheckFn, [], Columns) of
        []     -> true;
        Errors -> {false, lists:reverse(Errors)}
    end.

%% Reported error types must be supported by the function syntax_error_to_msg2
-spec is_insert_column_valid(module(), field_identifier(), list()) ->
                             list(true | query_syntax_error()).
is_insert_column_valid(Mod, {identifier, X}, Acc) ->
    case Mod:is_field_valid(X) of
        true  ->
            Acc;
        false ->
            [{unexpected_insert_field, hd(X)} | Acc]
    end;
is_insert_column_valid(_, Other, Acc) ->
    [{unexpected_insert_field, Other} | Acc].

-spec are_insert_types_valid(module(), [insertion()], [[data_value()]]) ->
    true | {false, [true | query_syntax_error()]}.
are_insert_types_valid(Mod, Columns, Values) ->
    VerifyRowFn =
        fun(RowValues, Acc) ->
            [is_insert_row_type_valid(Mod, Columns, RowValues) | Acc]
        end,
    InvalidRows = lists:foldl(VerifyRowFn, [], Values),
    case lists:member(false, InvalidRows) of
        true  -> incompatible_insert_type;
        false -> true
    end.

-spec is_insert_row_type_valid(module(), [insertion()], [data_value()]) ->
    [] | [false].
is_insert_row_type_valid(Mod, Columns, RowValues) ->
    %% INSERT allows equal or less columns to be given if the INSERT statement
    %% specifies the columns in the value.
    case length(RowValues) > length(Columns) of
        true -> false;
        _ ->
            ColPos = build_insert_col_positions(Mod, Columns, RowValues),
            DataRow = build_insert_validation_obj(Mod, ColPos),
            Mod:validate_obj(DataRow)
    end.

-spec build_insert_col_positions(module(), [insertion()], [data_value()]) ->
    [{pos_integer(), term()}].
build_insert_col_positions(Mod, Columns, RowValues) ->
    BuildListFn =
        fun({{identifier, Col}, {_Type, Val}}, Acc) ->
            Pos = Mod:get_field_position(Col),
            [{Pos, Val} | Acc]
        end,
    Unsorted = lists:foldl(BuildListFn, [], match_columns_to_values(Columns, RowValues)),
    lists:keysort(1, Unsorted).

%% Make the list lengths match to allow construction of validation object
-spec match_columns_to_values([field_identifier()], [data_value()]) ->
                           [{field_identifier(), data_value()}].
match_columns_to_values(Cols, Vals) when length(Cols) == length(Vals) ->
    lists:zip(Cols, Vals);
match_columns_to_values(Cols, Vals) when length(Cols) > length(Vals) ->
    lists:zip(lists:sublist(Cols, 1, length(Vals)), Vals);
match_columns_to_values(Cols, Vals) when length(Cols) < length(Vals) ->
    lists:zip(Cols, lists:sublist(Vals, 1, length(Cols))).

-spec build_insert_validation_obj(module(), [{pos_integer(), term()}]) ->
    tuple().
build_insert_validation_obj(Mod, ColPos) ->
    Row = make_empty_insert_row(Mod),
    ExtractFn = fun({Pos, Val}, Acc) ->
        case is_integer(Pos) of
            true -> setelement(Pos, Acc, Val);
            _ -> Acc
        end
    end,
    lists:foldl(ExtractFn, Row, ColPos).

make_empty_insert_row(Mod) ->
    Positions = Mod:get_field_positions(),
    list_to_tuple(lists:duplicate(length(Positions), [])).

any_unknown_column(_Fields, _FInMod, true) ->
    true;
any_unknown_column([], [], _AnyUnknown) ->
    false;
any_unknown_column([], _FInMod, false) ->
    false;
any_unknown_column([{identifier, [F]}|FieldsT], FInMod, false) ->
    IsKnown = lists:any(fun(El) -> El =:= F end, FInMod),
    any_unknown_column(FieldsT, FInMod, not IsKnown).

%% If the INSERT command specifies only a partial list of columns, expand the
%% list of columns to include those known by the DDL, ensuring that the values
%% provided and those omitted align with the columns as provided by the caller.
-spec insert_sql_columns(module(), [field_identifier()], [data_value()]) ->
    {[field_identifier()],[[data_value()]]}.
insert_sql_columns(Mod, Fields, Values) when is_atom(Mod) ->
    FInMod = [ F || {[F], _I} <- Mod:get_field_positions() ],
    case any_unknown_column(Fields, FInMod, false) of
        true ->
            %% will fail downstream
            {Fields, Values};
        _ ->
            FInQuery0 = lists:flatten(proplists:get_all_values(identifier, Fields)),
            FInQuery1 = insert_sql_columns_fields(FInMod, FInQuery0),
            VInQuery1 = [ insert_sql_columns_row_values(FInQuery1, V) ||
                V <- Values ],

            %% rearrange into DDL Module order
            in_ddl_order(FInQuery1, VInQuery1, FInMod, Mod)
    end.

%% fields in query match fields in ddl, optimization to not rearrange
in_ddl_order(FD, VQs, FD, _Mod) ->
    {[{identifier, [F]} || F <- FD ], VQs};
%% no values, all null, will likely fail downstream
in_ddl_order(FQ, _VQs=[], FD, Mod) ->
    VQs = [ {null, ?SQL_NULL} || _F <- FQ ],
    in_ddl_order(FQ, VQs, FD, Mod);
%% fields in query do not match fields in ddl
in_ddl_order(FQ, VQs=[_VQH=[_VH|_VT]|_VQT], FD, Mod) ->
    FVs = [ in_ddl_order_(FQ, VQ, FD, Mod, [], [], FQ, VQ) || VQ <- VQs ],
    Fields = [ {identifier, [F]} || F <- element(1, hd(FVs)) ],
    Values = [ element(2, FV) || FV <- FVs ],
    {Fields, Values}.

%% exhausted FD -> return to caller
in_ddl_order_(_FQ, _VQ, _FD=[], _Mod, FAcc, VAcc, _AFQ, _AVQ) ->
    {lists:reverse(FAcc), lists:reverse(VAcc)};
%% exhausted FQ, VQ -> append null and swap in AFQ and AVQ, traverse ddl field
in_ddl_order_(_FQ=[], _VQ=[], _FD=[HFD|TFD], Mod, FAcc, VAcc, AFQ, AVQ) ->
    NullFV = {Mod:get_field_type([HFD]), ?SQL_NULL},
    in_ddl_order_(AFQ, AVQ, TFD, Mod, [HFD|FAcc], [NullFV|VAcc], AFQ, AVQ);
%% matched HFQ and HFD -> append and swap in AFQ and AVQ, traverse ddl field
in_ddl_order_(_FQ=[HFQ|_TFQ], _VQ=[HVQ|_TVQ], _FD=[HFD|TFD], Mod, FAcc, VAcc, AFQ, AVQ) when HFQ =:= HFD ->
    in_ddl_order_(AFQ, AVQ, TFD, Mod, [HFQ|FAcc], [HVQ|VAcc], AFQ, AVQ);
%% not matched -> traverse query field/value
in_ddl_order_(_FQ=[_HFQ|TFQ], _VQ=[_HVQ|TVQ], FD=[_HFD|_TFD], Mod, FAcc, VAcc, AFQ, AVQ) ->
    in_ddl_order_(TFQ, TVQ, FD, Mod, FAcc, VAcc, AFQ, AVQ).

%% Get all of the columns, preferring the query order and fleshing out the
%% remainder with those defined in the DDL Module.
insert_sql_columns_fields(FInMod, FInQuery0) ->
    FInModLength = length(FInMod),
    FInQuery0Length = length(FInQuery0),
    case FInQuery0Length of
        0 -> FInMod;
        FInModLength -> FInQuery0;
        _ -> FInQuery0 ++
            [F || F <- FInMod, not lists:any(fun(FInQ) -> F =:= FInQ end, FInQuery0) ]
    end.

insert_sql_columns_row_values(FInQuery, Values) ->
    Values1 = case length(Values) =< length(FInQuery) of
        false -> Values;
        _ ->
            case length(Values) =:= length(FInQuery) of
                true -> Values;
                _ ->
                    Values ++
                    [ {null, ?SQL_NULL} || _I <- lists:seq(length(Values) + 1, length(FInQuery)) ]
            end
    end,
    lists:map(fun ({Type, Value}) ->
                case {Type, Value} of
                    %% implicit NULL
                    {null, ?SQL_NULL} -> {null, ?SQL_NULL};
                    %% explicit NULL
                    {null, <<_Null/binary>>} -> {null, ?SQL_NULL};
                    {Type, Value} -> {Type, Value}
                end
        end,
        Values1).

%% Get the type of a field from the DDL datastructure.
%%
%% NOTE: If a compiled helper module is a available then use
%% `Mod:get_field_type/1'.
-spec get_field_type(?DDL{}, binary()) -> {ok, external_field_type()} | notfound.
get_field_type(?DDL{ fields = Fields }, FieldName) when is_binary(FieldName) ->
    case lists:keyfind(FieldName, #riak_field_v1.name, Fields) of
      #riak_field_v1{ type = Type } ->
          {ok, Type};
      false ->
            notfound
    end.

%% Get the storage data type for a type found in a DDL. Typically
%% they'll be the same, but `blob' e.g. maps to a `varchar'.
-spec get_storage_type(external_field_type()) -> internal_field_type().
get_storage_type(blob) -> varchar;
get_storage_type(Type) -> Type.

%% Get an equivalent data type for a type found in a DDL to allow the
%% DDL compiler to create a safe module for older versions of TS
-spec get_legacy_type(external_field_type(), atom()) -> external_field_type().
get_legacy_type(blob, v1) -> varchar;
get_legacy_type(Type, _Version) -> Type.

-spec get_minimum_capability(any_ddl()) -> ddl_version().
get_minimum_capability(#ddl_v1{}) ->
    v1;
get_minimum_capability(DDL) ->
    %% Determing the minimum capability value that is required to run
    %% operations on this table. For each function in this list, be
    %% explicit about the DDL record (use `#ddl_v<n>' instead of the
    %% `?DDL' macro). DDL v1 records will never be passed to any of these
    %% functions
    lists:max([Fn(DDL) || Fn <- [fun get_descending_keys_required_cap/1,
                                 fun get_type_required_cap/1]]).

%%
get_type_required_cap(#ddl_v2{fields=RiakFields}) ->
    lists:max(lists:map(fun(#riak_field_v1{type=blob}) -> v2;
                           (_) -> v1
                        end, RiakFields)).

%%
get_descending_keys_required_cap(#ddl_v2{ local_key = #key_v1{ ast = AST } }) ->
    lists:foldl(fun get_descending_keys_required_cap_fold/2, v1, AST).

%%
get_descending_keys_required_cap_fold(?SQL_PARAM{ ordering = descending }, Acc) ->
    max_version(v2, Acc);
get_descending_keys_required_cap_fold(_, Acc) ->
    Acc.

%%
max_version(A,B) when A == v2 orelse B == v2 -> v2;
max_version(_,_) -> v1.

%%% -------------------------------------------------------------------
%%% DDL UPGRADES
%%% -------------------------------------------------------------------

first_version() ->
    v1.

current_version() ->
    ?DDL_RECORD_VERSION.

%% Convert a ddl record to a different version.
-spec convert(ddl_version(), DDL::any_ddl()) -> [any_ddl()|{error,{cannot_downgrade,ddl_version()}}].
convert(Version, DDL) when is_atom(Version) ->
    CurrentVersion = ddl_record_version(element(1, DDL)),
    case is_version_greater(Version, CurrentVersion) of
        equal ->
            [DDL];
        true  ->
            VersionSteps = sublist_elements(CurrentVersion, Version, [v1,v2]),
            upgrade_ddl(VersionSteps, DDL);
        false ->
            [V1,V2] = sublist_elements(CurrentVersion, Version, [v2,v1]),
            downgrade_ddl(V1,V2, DDL)
    end.

%%
-spec is_version_greater(ddl_version(), ddl_version()) -> equal | boolean().
is_version_greater(V, V)  -> equal;
is_version_greater(v1,v2) -> false;
is_version_greater(v2,v1) -> true.

%%
ddl_record_version(ddl_v1) -> v1;
ddl_record_version(ddl_v2) -> v2;
ddl_record_version(V) when is_atom(V)
                           -> throw({unknown_ddl_version, V}).

%%
downgrade_ddl(v2, v1, DDL1) ->
    case get_minimum_capability(DDL1) of
        v1 ->
            [#ddl_v1{
                            table              = DDL1#ddl_v2.table,
                            fields             = lists:map(
                                                   fun(F) -> downgrade_field(F, v1) end,
                                                   DDL1#ddl_v2.fields),
                            partition_key      = downgrade_v2_key(DDL1#ddl_v2.partition_key),
                            local_key          = downgrade_v2_key(DDL1#ddl_v2.local_key)
                        }];
        _ ->
            [{error, {cannot_downgrade, v1}}]
    end.

%% Downgrade fields to legacy types
downgrade_field(#riak_field_v1{type=Type}=Field, OldVersion) ->
    Field#riak_field_v1{type=get_legacy_type(Type, OldVersion)}.

%% Downgrade a v2 local key to v1, the key should already be checked that
%% it is safe to downgrade before it is called.
downgrade_v2_key(#key_v1{ ast = AST }) ->
    #key_v1{ ast =
        [downgrade_v2_param(X) || X <- AST]
    }.

%%
downgrade_v2_param(#param_v2{name = N}) ->
    #param_v1{name = N};
downgrade_v2_param(#hash_fn_v1{args = Args} = HashFn) ->
    HashFn#hash_fn_v1{
        args = [downgrade_v2_param(X) || X <- Args]
    };
downgrade_v2_param(X) ->
    %% hash_fn arguments can be any kind of term
    X.

%%
upgrade_ddl([_], DDL) ->
    [DDL];
upgrade_ddl([v1,v2 = To|Tail], DDL1) ->
    DDL2 = #ddl_v2{
        table              = DDL1#ddl_v1.table,
        fields             = DDL1#ddl_v1.fields,
        partition_key      = upgrade_v1_key(DDL1#ddl_v1.partition_key),
        local_key          = upgrade_v1_key(DDL1#ddl_v1.local_key),
        minimum_capability = ddl_minimum_capability(DDL1, v1)
    },
    DDL3 = DDL2#ddl_v2{
        minimum_capability = ddl_minimum_capability(DDL2, v1)
    },
    [DDL1 | upgrade_ddl([To|Tail], DDL3)].

%%
upgrade_v1_key(#key_v1{ ast = AST } = Key) ->
    Key#key_v1{ast =
        [upgrade_v1_param(X) || X <- AST]
    }.

%% upgrade param_v1 records to v2, if they are not this type of record then
%% just return it, since it is a hash_fn.
upgrade_v1_param(#param_v1{name = N}) ->
    #param_v2{name = N, ordering = ascending};
upgrade_v1_param(#hash_fn_v1{args = Args} = HashFn) ->
    HashFn#hash_fn_v1{
        args = [upgrade_v1_param(X) || X <- Args]
    };
upgrade_v1_param(X) ->
    %% hash_fn arguments can be any kind of term
    X.

%%
ddl_minimum_capability(DDL, DefaultMin) ->
    Min = get_minimum_capability(DDL),
    case is_version_greater(Min, DefaultMin) of
        false -> Min;
        _     -> DefaultMin
    end.

%% Return a sublist within a list when only the start and end elements within
%% the list are known, not the index.
sublist_elements(_, _, []) ->
    [];
sublist_elements(From, To, [From|Tail]) ->
    [From|sublist_elements_inner(To, Tail)];
sublist_elements(From, To, [_|Tail]) ->
    sublist_elements(From, To, Tail).

%%
sublist_elements_inner(_, []) ->
    [];
sublist_elements_inner(To, [To|_]) ->
    [To];
sublist_elements_inner(To, [Other|    Tail]) ->
    [Other|sublist_elements_inner(To, Tail)].


%%% -------------------------------------------------------------------
%%% TESTS
%%% -------------------------------------------------------------------

-ifdef(TEST).
-compile(export_all).

-define(VALID,   true).
-define(INVALID, false).

-include_lib("eunit/include/eunit.hrl").

make_module_name_1_test() ->
    ?assertEqual(
        list_to_atom("riak_ql_table_mytab_" ++ integer_to_list(riak_ql_ddl_compiler:get_compiler_version())),
        make_module_name(<<"mytab">>)
    ).

make_module_name_2_test() ->
    ?assertEqual(riak_ql_table_fafa_1, make_module_name(<<"fafa">>, 1)).

%% upper case, underscores and numbers ok
make_module_name_3_test() ->
    ?assertEqual(riak_ql_table_MY_TAB12345_1, make_module_name(<<"MY_TAB12345">>, 1)).



%%
%% Helper Fn for unit tests
%%

mock_partition_fn(_A, _B, _C) -> mock_result.

make_ddl(Table, Fields, #key_v1{} = PK, #key_v1{} = LK) when is_binary(Table) ->
    ?DDL{table         = Table,
         fields        = Fields,
         partition_key = PK,
         local_key     = LK}.

%%
%% get partition_key tests
%%

simplest_partition_key_test() ->
    Name = <<"yando">>,
    Key = #key_v1{ast = [?SQL_PARAM{name = [Name]}]},
    DDL = make_ddl(<<"simplest_partition_key_test">>,
                   [
                    #riak_field_v1{name     = Name,
                                   position = 1,
                                   type     = varchar}
                   ],
                   Key,
                   Key),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Obj = {Name},
    Result = (catch get_partition_key(DDL, Obj)),
    ?assertEqual([{varchar, Name}], Result).

simple_partition_key_test() ->
    Name1 = <<"yando">>,
    Name2 = <<"buckle">>,
    Key = #key_v1{ast = [
                        ?SQL_PARAM{name = [Name1]},
                        ?SQL_PARAM{name = [Name2]}
                       ]},
    DDL = make_ddl(<<"simple_partition_key_test">>,
                   [
                    #riak_field_v1{name     = Name2,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"sherk">>,
                                   position = 2,
                                   type     = varchar},
                    #riak_field_v1{name     = Name1,
                                   position = 3,
                                   type     = varchar}
                   ],
                   Key,
                   Key),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Obj = {<<"one">>, <<"two">>, <<"three">>},
    Result = (catch get_partition_key(DDL, Obj)),
    ?assertEqual([{varchar, <<"three">>}, {varchar, <<"one">>}], Result).

%%
%% get local_key tests
%%

local_key_test() ->
    Name = <<"yando">>,
    PK = #key_v1{ast = [
                        ?SQL_PARAM{name = [Name]}
                       ]},
    LK = #key_v1{ast = [
                        ?SQL_PARAM{name = [Name]}
                       ]},
    DDL = make_ddl(<<"simplest_key_key_test">>,
                   [
                    #riak_field_v1{name     = Name,
                                   position = 1,
                                   type     = varchar}
                   ],
                   PK, LK),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Obj = {Name},
    Result = (catch get_local_key(DDL, Obj)),
    ?assertEqual([{varchar, Name}], Result).


%%
%% make_key tests
%%

make_plain_key_test() ->
    Key = #key_v1{ast = [
                         ?SQL_PARAM{name = [<<"user">>]},
                         ?SQL_PARAM{name = [<<"time">>]}
                        ]},
    DDL = make_ddl(<<"make_plain_key_test">>,
                   [
                    #riak_field_v1{name     = <<"user">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"time">>,
                                   position = 2,
                                   type     = timestamp}
                   ],
                   Key, %% use the same key for both
                   Key),
    Time = 12345,
    Vals = [
            {<<"user">>, <<"user_1">>},
            {<<"time">>, Time}
           ],
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Got = make_key(Mod, Key, Vals),
    Expected = [{varchar, <<"user_1">>}, {timestamp, Time}],
    ?assertEqual(Expected, Got).

make_functional_key_test() ->
    Table_def =
        "CREATE TABLE make_plain_key_test ("
        "user VARCHAR NOT NULL, "
        "time TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((user, quantum(time, 15, 'm')), user, time))",
    {ddl, DDL, _} =
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def)),
    Vals = [
            {<<"user">>, <<"user_1">>},
            {<<"time">>, 12345}
           ],
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Got = make_key(Mod, DDL?DDL.local_key, Vals),
    Expected = [{varchar, <<"user_1">>}, {timestamp, 12345}],
    ?assertEqual(Expected, Got).

helper_compile_def_to_module(SQL) ->
    Lexed = riak_ql_lexer:get_tokens(SQL),
    {ok, {DDL, _Props}} = riak_ql_parser:parse(Lexed),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    {DDL, Mod}.

% basic family/series/timestamp
make_ts_keys_1_test() ->
    {DDL, Mod} = helper_compile_def_to_module(
        "CREATE TABLE table1 ("
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY((a, b, quantum(c, 15, 's')), a, b, c))"),
    ?assertEqual(
        {ok, [1,2,0]},
        lk_to_pk([1,2,3], Mod, DDL)
    ).

% a two element key, still using the table definition field order
make_ts_keys_2_test() ->
    {DDL, Mod} = helper_compile_def_to_module(
        "CREATE TABLE table1 ("
        "a SINT64 NOT NULL, "
        "b TIMESTAMP NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY((a, quantum(b, 15, 's')), a, b))"),
    ?assertEqual(
        {ok, [1,0]},
        lk_to_pk([1,2], Mod, DDL)
    ).

make_ts_keys_3_test() ->
    {DDL, Mod} = helper_compile_def_to_module(
        "CREATE TABLE table2 ("
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "d SINT64 NOT NULL, "
        "PRIMARY KEY  ((d,a,quantum(c, 1, 's')), d,a,c))"),
    ?assertEqual(
        {ok, [10,20,0]},
        lk_to_pk([10,20,1], Mod, DDL)
    ).

make_ts_keys_4_test() ->
    {DDL, Mod} = helper_compile_def_to_module(
        "CREATE TABLE table2 ("
        "ax SINT64 NOT NULL, "
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "d SINT64 NOT NULL, "
        "PRIMARY KEY  ((ax,a,quantum(c, 1, 's')), ax,a,c))"),
    ?assertEqual(
        {ok, [10,20,0]},
        lk_to_pk([10,20,1], Mod, DDL)
    ).

%%
%% Validate Query Tests
%%

partial_wildcard_check_selections_valid_test() ->
    {ddl, DDL, _} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
        "CREATE TABLE partial_wildcard_check_selections_valid_test("
        "temperature SINT64 NOT NULL, "
        "geohash SINT64 NOT NULL, "
        "PRIMARY KEY ((temperature, geohash), temperature, geohash))"
    )),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    ?assertEqual(
         true,
         are_selections_valid(Mod, [{identifier, [<<"*">>]}], ?CANTBEBLANK)
    ).

%%
%% Query Validation tests
%%

simple_is_query_valid_test() ->
    {ddl, DDL, _} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
        "CREATE TABLE simple_is_query_valid_test("
        "temperature SINT64 NOT NULL, "
        "geohash SINT64 NOT NULL, "
        "PRIMARY KEY ((temperature, geohash), temperature, geohash))"
    )),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Bucket = <<"simple_is_query_valid_test">>,
    Selections  = [{identifier, [<<"temperature">>]}, {identifier, [<<"geohash">>]}],
    Query = {Bucket, Selections, [], undefined},
    ?assertEqual(
       true,
       riak_ql_ddl:is_query_valid(Mod, DDL, Query)
      ).

%%
%% Tests for queries with non-null filters
%%

simple_filter_query_test() ->
    Bucket = <<"simple_filter_query_test">>,
    Selections = [{identifier, [<<"temperature">>]}, {identifier, [<<"geohash">>]}],
    Where = [
             {and_,
              {'>', <<"temperature">>, {integer, 1}},
              {'<', <<"temperature">>, {integer, 15}}
             }
            ],
    Query = {Bucket, Selections, Where, undefined},
    {ddl, DDL, _} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
        "CREATE TABLE simple_filter_query_test("
        "temperature SINT64 NOT NULL, "
        "geohash SINT64 NOT NULL, "
        "PRIMARY KEY ((temperature, geohash), temperature, geohash))"
    )),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    ?assertEqual(true, riak_ql_ddl:is_query_valid(Mod, DDL, Query)).

full_filter_query_test() ->
    Bucket = <<"simple_filter_query_test">>,
    Selections = [{identifier, [<<"temperature">>]}],
    Where = [
             {and_,
              {'>', <<"temperature">>, {integer, 1}},
              {and_,
               {'<', <<"temperature">>, {integer, 15}},
               {or_,
                {'!=', <<"ne field">>,   {integer, 15}},
                {and_,
                 {'<=', <<"lte field">>,  {integer, 15}},
                 {'>=', <<"gte field">>,  {integer, 15}}}}}}
            ],
    Query = {Bucket, Selections, Where, undefined},
    {ddl, DDL, _} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
        "CREATE TABLE simple_filter_query_test("
        "temperature SINT64 NOT NULL, "
        "geohash SINT64 NOT NULL, "
        "\"ne field\" SINT64 NOT NULL, "
        "\"lte field\" SINT64 NOT NULL, "
        "\"gte field\" SINT64 NOT NULL, "
        "PRIMARY KEY ((temperature, geohash), temperature, geohash))"
    )),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    ?assertEqual(true, riak_ql_ddl:is_query_valid(Mod, DDL, Query)).


timeseries_filter_test() ->
    Bucket = <<"timeseries_filter_test">>,
    Selections = [{identifier, [<<"weather">>]}],
    Where = [
             {and_,
              {and_,
               {'>', <<"time">>, {integer, 3000}},
               {'<', <<"time">>, {integer, 5000}}
              },
              {'=', <<"user">>, {binary, <<"user_1">>}
              }
             }
            ],
    Query = {Bucket, Selections, Where, undefined},
    Fields = [
              #riak_field_v1{name     = <<"geohash">>,
                             position = 1,
                             type     = varchar,
                             optional = false},
              #riak_field_v1{name     = <<"user">>,
                             position = 2,
                             type     = varchar,
                             optional = false},
              #riak_field_v1{name     = <<"time">>,
                             position = 3,
                             type     = timestamp,
                             optional = false},
              #riak_field_v1{name     = <<"weather">>,
                             position = 4,
                             type     = varchar,
                             optional = false},
              #riak_field_v1{name     = <<"temperature">>,
                             position = 5,
                             type     = varchar,
                             optional = true}
             ],
    PK = #key_v1{ast = [
                        #hash_fn_v1{mod  = riak_ql_quanta,
                                    fn   = quantum,
                                    args = [
                                            ?SQL_PARAM{name = [<<"time">>]},
                                            15,
                                            s
                                           ]}
                       ]},
    LK = #key_v1{ast = [
                        ?SQL_PARAM{name = [<<"time">>]},
                        ?SQL_PARAM{name = [<<"user">>]}]
                },
    DDL = ?DDL{table         = <<"timeseries_filter_test">>,
               fields        = Fields,
               partition_key = PK,
               local_key     = LK
              },
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Res = riak_ql_ddl:is_query_valid(Mod, DDL, Query),
    Expected = true,
    ?assertEqual(Expected, Res).

%% is_query_valid expects a 4-tuple: table name, fields, where, order_by clause
parsed_sql_to_query(Proplist) ->
    {
      proplists:get_value(tables, Proplist, <<>>),
      proplists:get_value(fields, Proplist, []),
      proplists:get_value(where, Proplist, []),
      proplists:get_value(order_by, Proplist)
    }.

%% is_query_valid expects a 3-tuple: table name, fields, values
parsed_sql_to_insert(Mod, Proplist) ->
    Table = proplists:get_value(table, Proplist, <<>>),
    ValuesInQuery = proplists:get_value(values, Proplist, []),
    FieldsInQuery = proplists:get_value(fields, Proplist, []),
    {Fields, Values} = insert_sql_columns(Mod, FieldsInQuery, ValuesInQuery),
    {
        Table,
        Fields,
        Values
    }.

test_parse(SQL) ->
    case riak_ql_parser:ql_parse(
           riak_ql_lexer:get_tokens(SQL)) of
        {ddl, Parsed, _Props} ->
            Parsed;
        {_Species, Parsed} ->
            Parsed
    end.

is_sql_valid_test_helper(Table_name, Table_def) ->
    Mod_name = make_module_name(iolist_to_binary(Table_name)),
    catch code:purge(Mod_name),
    catch code:purge(Mod_name),
    DDL = test_parse(Table_def),
    %% ?debugFmt("QUERY is ~p", [test_parse(Query)]),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    {DDL, Mod}.

is_query_valid_test_helper(Table_name, Table_def, Query) ->
    {DDL, Mod} = is_sql_valid_test_helper(Table_name, Table_def),
    is_query_valid(Mod, DDL, parsed_sql_to_query(test_parse(Query))).

is_insert_valid_test_helper(Table_name, Table_def, Insert) ->
    {DDL, Mod} = is_sql_valid_test_helper(Table_name, Table_def),
    is_insert_valid(Mod, DDL, parsed_sql_to_insert(Mod, test_parse(Insert))).

-define(LARGE_TABLE_DEF,
        "CREATE TABLE mytab"
        "   (myfamily    VARCHAR   NOT NULL, "
        "    myseries    VARCHAR   NOT NULL, "
        "    time        TIMESTAMP NOT NULL, "
        "    weather     VARCHAR   NOT NULL, "
        "    temperature DOUBLE, "
        "    PRIMARY KEY ((myfamily, myseries, QUANTUM(time, 15, 'm')), "
        "    myfamily, myseries, time))"
       ).

insert_sql_columns_empty_fields_test() ->
    {_DDL, Mod} = is_sql_valid_test_helper("mytab", ?LARGE_TABLE_DEF),
    FInMod = [ {identifier,[F]} || {[F], _I} <- Mod:get_field_positions() ],
    FieldsIn = [],
    ValuesIn = [[{varchar, <<"family1">>},
            {varchar, <<"series1">>},
            {timestamp, 1000},
            {varchar, <<"sunny">>},
            {double, 37.0}]],
    {FieldsOut, ValuesOut } = insert_sql_columns(Mod, FieldsIn, ValuesIn),
    ?assertEqual(FInMod, FieldsOut),
    ?assertEqual(ValuesIn, ValuesOut).

insert_sql_columns_pk_fields_test() ->
    {_DDL, Mod} = is_sql_valid_test_helper("mytab", ?LARGE_TABLE_DEF),
    FInMod = [ {identifier,[F]} || {[F], _I} <- Mod:get_field_positions() ],
    FieldsIn = lists:sublist(FInMod, 3),
    ValuesIn = [[{varchar, <<"family1">>},
            {varchar, <<"series1">>},
            {timestamp, 1000}]],
    {FieldsOut, ValuesOut } = insert_sql_columns(Mod, FieldsIn, ValuesIn),
    %% expecting values specified plus nulls
    ValuesExpected = [hd(ValuesIn) ++
        lists:sublist(hd(ValuesOut), length(FieldsIn) + 1, length(FInMod) - length(FieldsIn))],
    ?assertEqual(FInMod, FieldsOut),
    ?assertEqual(ValuesExpected, ValuesOut).

insert_sql_columns_all_fields_test() ->
    {_DDL, Mod} = is_sql_valid_test_helper("mytab", ?LARGE_TABLE_DEF),
    FInMod = [ {identifier,[F]} || {[F], _I} <- Mod:get_field_positions() ],
    FieldsIn = FInMod,
    ValuesIn = [[{varchar, <<"family1">>},
            {varchar, <<"series1">>},
            {timestamp, 1000},
            {varchar, <<"sunny">>},
            {double, 37.0}]],
    {FieldsOut, ValuesOut} = insert_sql_columns(Mod, FieldsIn, ValuesIn),
    ?assertEqual(FInMod, FieldsOut),
    ?assertEqual(ValuesIn, ValuesOut).

%% Weave two equal length lists equivalently, ensuring a reordering of the
%% elements, but in a deterministic way compared to random shuffle.
weave2(L0, L1) ->
    weave2(list_to_tuple(L0), list_to_tuple(L1), length(L0)).

weave2(L0, L1, 0) ->
    {tuple_to_list(L0), tuple_to_list(L1)};
weave2(L0, L1, Len) ->
    Rand = case Len rem 3 of
               0 -> 2;
               1 -> 1;
               2 -> 3
           end,
    A0 = element(Len, L0),
    A1 = element(Len, L1),
    B0 = element(Rand, L0),
    B1 = element(Rand, L1),
    L01 = setelement(Len, L0, B0),
    L11 = setelement(Len, L1, B1),
    L02 = setelement(Rand, L01, A0),
    L12 = setelement(Rand, L11, A1),
    weave2(L02, L12, Len - 1).

insert_sql_columns_waaved_fields_test() ->
    {_DDL, Mod} = is_sql_valid_test_helper("mytab", ?LARGE_TABLE_DEF),
    FInMod = [ {identifier,[F]} || {[F], _I} <- Mod:get_field_positions() ],
    FieldsIn0 = FInMod,
    ValuesIn0 = [[{varchar, <<"family1">>},
            {varchar, <<"series1">>},
            {timestamp, 1000},
            {varchar, <<"sunny">>},
            {double, 37.0}]],
    {FieldsIn,ValuesIn1} = weave2(FieldsIn0, hd(ValuesIn0)),
    ValuesIn = [ValuesIn1],
    {FieldsOut, ValuesOut} = insert_sql_columns(Mod, FieldsIn, ValuesIn),
    ?assertEqual(FInMod, FieldsOut),
    %% !IMPORTANT! values out should be in record order, not query order
    ?assertEqual(ValuesIn0, ValuesOut).

insert_sql_columns_multiple_rows_test() ->
    {_DDL, Mod} = is_sql_valid_test_helper("mytab", ?LARGE_TABLE_DEF),
    FInMod = [ {identifier,[F]} || {[F], _I} <- Mod:get_field_positions() ],
    FieldsIn = FInMod,
    ValuesIn = [
        [{varchar, list_to_binary("family" ++ integer_to_list(I))},
         {varchar, list_to_binary("series" ++ integer_to_list(I))},
         {timestamp, I * 1000},
         {varchar, <<"sunny">>},
         {double, case I rem 3 of
                    0 -> 37.0;
                    1 -> 33.3;
                    _ -> 34.2
                  end}]
        || I <- lists:seq(1, 10)
    ],
    {FieldsOut, ValuesOut } = insert_sql_columns(Mod, FieldsIn, ValuesIn),
    ?assertEqual(FieldsIn, FieldsOut),
    ?assertEqual(ValuesIn, ValuesOut).

is_query_valid_1_test() ->
    ?assertEqual(
       true,
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 10 AND time < 11")
      ).

is_query_valid_3_test() ->
    ?assertEqual(
       true,
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 10 AND time < 11"
                                  "AND myseries = 'bob'")
      ).

is_query_valid_4_test() ->
    ?assertEqual(
       true,
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 10 AND time < 11"
                                  "AND myseries != 'bob'")
      ).

is_query_valid_where_1_test() ->
    ?assertEqual(
       {false, [
                {unexpected_where_field, <<"locname">>}]},
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 10 AND time < 11 AND locname = 1")
      ).

is_query_valid_where_2_test() ->
    ?assertEqual(
       {false, [
                {incompatible_type, <<"myseries">>, varchar, integer}]},
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 1 AND time < 10 "
                                  "AND myfamily = 'family1' "
                                  "AND myseries = 10 ")
      ).

is_query_valid_where_3_test() ->
    ?assertEqual(
       {false, [
                {incompatible_type, <<"myfamily">>, varchar, integer},
                {incompatible_type, <<"myseries">>, varchar, integer}]},
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 1 AND time < 10 "
                                  "AND myfamily = 12 "
                                  "AND myseries = 10 ")
      ).

is_query_valid_where_4_test() ->
    ?assertEqual(
       true,
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 1 AND time < 10 "
                                  "AND myfamily = 'bob' "
                                  "OR myseries = 'bert' ")
      ).

is_query_valid_where_5_test() ->
    ?assertEqual(
       true,
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 1 AND time < 10 "
                                  "AND myfamily = 'bob' "
                                  "OR myfamily = 'bert' ")
      ).

is_query_valid_where_6_test() ->
    ?assertEqual(
       true,
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 1 AND time < 10 "
                                  "AND myfamily = 'bob' "
                                  "AND myfamily = 'bert' ")
       %% FIXME contradictory where clause, this will never match
      ).

is_query_valid_selections_1_test() ->
    ?assertEqual(
       true,
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT myseries FROM mytab "
                                  "WHERE time > 1 AND time < 10 ")
      ).

is_query_valid_selections_2_test() ->
    ?assertEqual(
       {false, [{unexpected_select_field,<<"doge">>}]},
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT doge FROM mytab "
                                  "WHERE time > 1 AND time < 10 ")
      ).

is_query_valid_selections_3_test() ->
    ?assertEqual(
       {false, [
                {unexpected_select_field,<<"doge">>},
                {unexpected_select_field,<<"nyan">>}]},
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT doge, nyan FROM mytab "
                                  "WHERE time > 1 AND time < 10 ")
      ).

is_query_valid_select_and_where_1_test() ->
    ?assertEqual(
       {false, [
                {unexpected_select_field,<<"doge">>},
                {unexpected_select_field,<<"nyan">>},
                {unexpected_where_field,<<"monfamily">>}]},
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT doge, nyan FROM mytab "
                                  "WHERE time > 1 AND time < 10 "
                                  "AND monfamily = 12 ")
      ).

is_query_valid_compatible_op_1_test() ->
    ?assertEqual(
       {false, [
                {incompatible_operator, <<"myfamily">>, varchar, '>'}]},
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 1 AND time < 10 "
                                  "AND myfamily > 'bob' ")
      ).

is_query_valid_compatible_op_2_test() ->
    ?assertEqual(
       {false, [
                {incompatible_operator, <<"myfamily">>, varchar, '>='}]},
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 1 AND time < 10 "
                                  "AND myfamily >= 'bob' ")
      ).

is_query_valid_no_subexpressions_1_test() ->
    ?assertEqual(
       {false, [
                {subexpressions_not_supported, <<"time">>, '>'}]},
       is_query_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
                                  "SELECT * FROM mytab "
                                  "WHERE time > 1 + 2 AND time < 10 "
                                  "AND myfamily = 'bob' ")
      ).

is_insert_valid_null_test() ->
    ?assertEqual(
        true,
        is_insert_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
            "INSERT INTO mytab "
            "(myfamily, myseries, time, weather, temperature) VALUES"
            "('hazen', 'world', 15, 'nully', NULL)")).

is_insert_valid_1_test() ->
    ?assertEqual(
        true,
        is_insert_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
            "INSERT INTO mytab "
            "(myfamily, myseries, time, weather) VALUES"
            "('hazen', 'world', 15, 'sunny')")).

is_insert_valid_2_test() ->
    ?assertEqual(
        true,
        is_insert_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
            "INSERT INTO mytab VALUES"
            "('hazen', 'world', 69, 'sunny', 45.0)")).

is_insert_valid_out_of_order_1_test() ->
    ?assertEqual(
        true,
        is_insert_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
            "INSERT INTO mytab "
            "(myfamily, myseries, weather, time) VALUES"
            "('hazen', 'world', 'sunny', 15)")).

is_insert_valid_wrong_type_1_test() ->
    ?assertEqual(
        incompatible_insert_type,
        is_insert_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
            "INSERT INTO mytab "
            "(myfamily, myseries, weather, time) VALUES"
            "('hazen', 'world', 4.5, 15)")).

is_insert_valid_wrong_type_2_test() ->
    ?assertEqual(
        incompatible_insert_type,
        is_insert_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
            "INSERT INTO mytab VALUES"
            "('hazen', 'world', 4.5, 15)")).


is_insert_valid_too_many_1_test() ->
    ?assertEqual(
        incompatible_insert_type,
        is_insert_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
            "INSERT INTO mytab VALUES"
            "('hazen', 'world', 45, 'cloudy', 4.5, 'haggis', 'kilt')")).

is_insert_valid_invalid_column_1_test() ->
    ?assertEqual(
        {false, [
                 {unexpected_insert_field,<<"peppermint">>}]},
        is_insert_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
            "INSERT INTO mytab "
            "(myfamily, myseries, peppermint, time) VALUES"
            "('hazen', 'world', 'cloudy', 15)")).

is_insert_valid_multiple_rows_test() ->
    ?assertEqual(
        true,
        is_insert_valid_test_helper("mytab", ?LARGE_TABLE_DEF,
            "INSERT INTO mytab VALUES"
            "('hazen', 'world', 70, 'sunny', 45.0),"
            "('hazen', 'world', 71, 'sunny', 44.0),"
            "('hazen', 'world', 72, 'sunny', 46.0)")).

fold_where_tree_test() ->
    Parsed = test_parse(
               "SELECT * FROM mytab "
               "WHERE time > 1 AND time < 10 "
               "AND myfamily = 'family1' "
               "AND myseries = 10 "),
    Where = proplists:get_value(where, Parsed),
    ?assertEqual(
       [<<"myseries">>, <<"myfamily">>, <<"time">>, <<"time">>],
       lists:reverse(fold_where_tree(
                                     fun(_, {_, Field, _}, Acc) -> [Field | Acc] end, [], Where))
      ).

%%
%% selection validity tests
%%

-define(select_test(Name, SelectClause, Expected),
        Name() ->
               CreateTab = "CREATE TABLE mytab" ++
                   "   (myfamily    VARCHAR   NOT NULL, " ++
                   "    myseries    VARCHAR   NOT NULL, " ++
                   "    time        TIMESTAMP NOT NULL, " ++
                   "    mysint64    SINT64    NOT NULL, " ++
                   "    mydouble    DOUBLE    NOT NULL, " ++
                   "    mybolean    BOOLEAN   NOT NULL, " ++
                   "    myvarchar   VARCHAR   NOT NULL, " ++
                   "    PRIMARY KEY ((myfamily, myseries, QUANTUM(time, 15, 'm')), " ++
                   "    myfamily, myseries, time))",
               SQL = "SELECT " ++ SelectClause ++ " " ++
                   "FROM mytab WHERE " ++
                   "myfamily = 'fam1' " ++
                   "and myseries = 'ser1' " ++
                   "and time > 1 and time < 10",
               DDL = test_parse(CreateTab),
               {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
               Q = test_parse(SQL),
               Selections = proplists:get_value(fields, Q),
               Got = are_selections_valid(Mod, Selections, ?CANTBEBLANK),
               ?assertEqual(Expected, Got)).

?select_test(simple_column_select_1_test, "*", true).

?select_test(simple_column_select_2_test, "mysint64", true).

?select_test(simple_column_select_3_test, "mysint64, mydouble", true).

?select_test(simple_column_select_fail_1_test, "rootbeer",
             {false, [
                      {unexpected_select_field, <<"rootbeer">>}
                     ]
             }).

?select_test(simple_column_select_fail_2_test, "mysint64, rootbeer, mydouble, deathsquad",
             {false, [
                      {unexpected_select_field, <<"rootbeer">>},
                      {unexpected_select_field, <<"deathsquad">>}
                     ]
             }).

?select_test(simple_agg_fn_select_1_test, "count(mysint64)", true).

?select_test(simple_agg_fn_select_2_test, "count(mysint64), avg(mydouble)", true).

?select_test(simple_agg_fn_select_fail_2_test, "count(mysint64, myboolean), avg(mysint64)",
             {false, [
                      {fn_called_with_wrong_arity, 'COUNT', 1, 2}
                     ]
             }).

upgrade_ddl_with_hash_fn_test() ->
    HashFn =
        #hash_fn_v1{mod  = ?MODULE,
                    fn   = mock_partition_fn,
                    args = [#param_v1{name = [<<"a">>]}, 15, m],
                    type = timestamp},
    DDL_v1 = #ddl_v1{
        table = <<"tab">>,
        fields = [#riak_field_v1{}],
        local_key = #key_v1{ ast = [HashFn] },
        partition_key = #key_v1{ ast = [] }
    },
    ?assertEqual(
        [DDL_v1,
         #ddl_v2{
            table = <<"tab">>,
            fields = [#riak_field_v1{}],
            local_key = #key_v1{ ast = [HashFn#hash_fn_v1{ args = [#param_v2{name = [<<"a">>], ordering = ascending}, 15, m] }] },
            partition_key = #key_v1{ ast = [] }
         }],
        convert(v2, DDL_v1)
    ).

upgrade_ddl_v1_test() ->
    DDL_v1 = #ddl_v1{
        table = <<"tab">>,
        fields = [#riak_field_v1{}],
        local_key = #key_v1{ ast = [#param_v1{ name = [<<"a">>] }] },
        partition_key = #key_v1{ ast = [] }

    },
    ?assertEqual(
        [DDL_v1,
         #ddl_v2{
            table = <<"tab">>,
            fields = [#riak_field_v1{}],
            local_key = #key_v1{ ast = [#param_v2{ name = [<<"a">>], ordering = ascending }] },
            partition_key = #key_v1{ ast = [] }
         }],
        convert(v2, DDL_v1)
    ).

downgrade_ddl_v2_test() ->
    DDL_v2 = #ddl_v2{
        table = <<"tab">>,
        fields = [#riak_field_v1{}],
        local_key = #key_v1{ ast = [#param_v2{ name = [<<"a">>], ordering = ascending }] },
        partition_key = #key_v1{ ast = [] }
    },
    ?assertEqual(
        [#ddl_v1{
            table = <<"tab">>,
            fields = [#riak_field_v1{}],
            local_key = #key_v1{ ast = [#param_v1{ name = [<<"a">>] }] },
            partition_key = #key_v1{ ast = [] }
        }],
        convert(v1, DDL_v2)
    ).

downgrade_ddl_v2_with_desc_test() ->
    DDL_v2 = #ddl_v2{
        table = <<"tab">>,
        fields = [#riak_field_v1{}],
        local_key = #key_v1{ ast = [#param_v2{ name = [<<"a">>], ordering = descending }] }
    },
    ?assertEqual(
        [{error, {cannot_downgrade, v1}}],
        convert(v1, DDL_v2)
    ).

get_minimum_capability_blob_test() ->
    {ddl, DDL, _} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
        "CREATE TABLE mytab1 ("
        "a BLOB NOT NULL, "
        "ts timestamp NOT NULL, "
        "PRIMARY KEY((quantum(ts,30,'d')), ts));")),
    ?assertEqual(
        v2,
        get_minimum_capability(DDL)
    ).

mapfold_where_tree_test() ->
    ?assertEqual(
        {{and_, a, b}, acc},
        mapfold_where_tree(
            fun(_, and_, Acc) -> {ok, Acc};
               (_, F, Acc)    -> {F, Acc} end,
            acc,
            {and_, a, b})
    ).

mapfold_where_tree_eliminate_leaf_test() ->
    ?assertEqual(
        {a, acc},
        mapfold_where_tree(
            fun(_, and_, Acc) -> {ok, Acc};
               (_, b, Acc)    -> {eliminate, Acc};
               (_, F, Acc)    -> {F, Acc} end,
            acc,
            {and_, a, b})
    ).

mapfold_where_tree_skip_branch_test() ->
    ?assertEqual(
        {{and_, {or_, a, c}, b}, acc},
        mapfold_where_tree(
            %% or_ is skipped os function clause if a or c is process because it
            %% should be skipped.
            fun(_, and_, Acc) -> {ok, Acc};
               (_, or_, Acc)  -> {skip, Acc};
               (_, b, Acc)    -> {b, Acc} end,
            acc,
            {and_, {or_, a, c}, b})
    ).

-endif.
