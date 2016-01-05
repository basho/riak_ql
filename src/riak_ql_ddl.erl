%% -------------------------------------------------------------------
%%
%% riak_ql_ddl: API module for the DDL
%%
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% this function can be used to work out which Module to use
-export([
         make_module_name/1, make_module_name/2
        ]).

-type ddl() :: #ddl_v1{}.
-export_type([ddl/0]).


%% a helper function for destructuring data objects
%% and testing the validity of field names
%% the generated helper functions cannot contain
%% record definitions because of the build cycle
%% so this function can be called out to to pick
%% apart the DDL records

-export([get_local_key/2, get_local_key/3]).
-export([get_partition_key/2, get_partition_key/3]).
-export([is_query_valid/3]).
%%-export([get_return_types_and_col_names/2]).
-export([make_key/3]).
-export([syntax_error_to_msg/1]).

-type query_syntax_error() ::
        {bucket_type_mismatch, DDL_bucket::binary(), Query_bucket::binary()} |
        {incompatible_type, Field::binary(), simple_field_type(), atom()} |
        {incompatible_operator, Field::binary(), simple_field_type(), relational_op()}  |
        {unexpected_where_field, Field::binary()} |
        {unexpected_select_field, Field::binary()} |
        {selections_cant_be_blank, []}.

-export_type([query_syntax_error/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% for debugging only
-export([make_ddl/2]).
-endif.

-define(CANBEBLANK,  true).
-define(CANTBEBLANK, false).

-spec make_module_name(Table::binary()) ->
            module().
%% @doc Generate a unique module name for Table at version 1. @see
%%      make_module_name/2.
make_module_name(Table) ->
    make_module_name(Table, 1).

-spec make_module_name(Table::binary(), Version::integer()) ->
            module().
%% @doc Generate a unique, but readable and recognizable, module name
%%      for Table at a certain Version, by 'escaping' non-ascii chars
%%      in Table a la C++.
make_module_name(Table, Version)
  when is_binary(Table), is_integer(Version) ->
    T4BL3 = << <<(maybe_mangle_char(C))/binary>> || <<C>> <= Table>>,
    ModName = <<"riak_ql_table_", T4BL3/binary, $$, (list_to_binary(integer_to_list(Version)))/binary>>,
    binary_to_atom(ModName, latin1).

maybe_mangle_char(C) when (C >= $a andalso C =< $z);
                          (C >= $A andalso C =< $Z);
                          (C == $_) ->
    <<C>>;
maybe_mangle_char(C) ->
    <<$%, (list_to_binary(integer_to_list(C)))/binary>>.


-spec get_partition_key(#ddl_v1{}, tuple(), module()) -> term().
get_partition_key(#ddl_v1{partition_key = PK}, Obj, Mod)
  when is_tuple(Obj) ->
    #key_v1{ast = Params} = PK,
    _Key = build(Params, Obj, Mod, []).

-spec get_partition_key(#ddl_v1{}, tuple()) -> term().
get_partition_key(#ddl_v1{table = T}=DDL, Obj)
  when is_tuple(Obj) ->
    Mod = make_module_name(T),
    get_partition_key(DDL, Obj, Mod).

-spec get_local_key(#ddl_v1{}, tuple(), module()) -> term().
get_local_key(#ddl_v1{local_key = LK}, Obj, Mod)
  when is_tuple(Obj) ->
    #key_v1{ast = Params} = LK,
    _Key = build(Params, Obj, Mod, []).

-spec get_local_key(#ddl_v1{}, tuple()) -> term().
get_local_key(#ddl_v1{table = T}=DDL, Obj)
  when is_tuple(Obj) ->
    Mod = make_module_name(T),
    get_local_key(DDL, Obj, Mod).

-spec make_key(atom(), #key_v1{} | none, list()) -> [{atom(), any()}].
make_key(_Mod, none, _Vals) -> [];
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
mk_k([#param_v1{name = [Nm]} | T1], Vals, Mod, Acc) ->
    {Nm, V} = lists:keyfind(Nm, 1, Vals),
    Ty = Mod:get_field_type([Nm]),
    mk_k(T1, Vals, Mod, [{Ty, V} | Acc]).

-spec extract(list(), [{any(), any()}], [any()]) -> any().
extract([], _Vals, Acc) ->
    lists:reverse(Acc);
extract([#param_v1{name = [Nm]} | T], Vals, Acc) ->
    {Nm, Val} = lists:keyfind(Nm, 1, Vals),
    extract(T, Vals, [Val | Acc]);
extract([Constant | T], Vals, Acc) ->
    extract(T, Vals, [Constant | Acc]).

-spec build([#param_v1{}], tuple(), atom(), any()) -> list().
build([], _Obj, _Mod, A) ->
    lists:reverse(A);
build([#param_v1{name = Nm} | T], Obj, Mod, A) ->
    Val = Mod:extract(Obj, Nm),
    Type = Mod:get_field_type(Nm),
    build(T, Obj, Mod, [{Type, Val} | A]);
build([#hash_fn_v1{mod  = Md,
                   fn   = Fn,
                   args = Args,
                   type = Ty} | T], Obj, Mod, A) ->
    A2 = convert(Args, Obj, Mod, []),
    Val = erlang:apply(Md, Fn, A2),
    build(T, Obj, Mod, [{Ty, Val} | A]).

-spec convert([#param_v1{}], tuple(), atom(), [any()]) -> any().
convert([], _Obj, _Mod, Acc) ->
    lists:reverse(Acc);
convert([#param_v1{name = Nm} | T], Obj, Mod, Acc) ->
    Val = Mod:extract(Obj, Nm),
    convert(T, Obj, Mod, [Val | Acc]);
convert([Constant | T], Obj, Mod, Acc) ->
    convert(T, Obj, Mod, [Constant | Acc]).

%% Convert an error emmitted from the :is_query_valid/3 function
%% and convert it into a user-friendly, text message binary.
-spec syntax_error_to_msg(query_syntax_error()) ->
            Msg::binary().
syntax_error_to_msg(E) ->
        {Fmt, Args} = syntax_error_to_msg2(E),
        iolist_to_binary(io_lib:format(Fmt, Args)).

%%
syntax_error_to_msg2({type_check_failed, Fn, Arity, ExprTypes}) ->
    {"Function ~p/~p called with arguments of the wrong type ~p.", [Fn, Arity, ExprTypes]};
syntax_error_to_msg2({fn_called_with_wrong_arity, Fn, Arity, NoArgs}) ->
    {"Function ~p/~p called with ~p arguments.", [Fn, Arity, NoArgs]};
syntax_error_to_msg2({bucket_type_mismatch, B1, B2}) ->
    {"bucket_type_mismatch: DDL bucket type was ~s "
     "but query selected from bucket type ~s.", [B1, B2]};
syntax_error_to_msg2({incompatible_type, Field, Expected, Actual}) ->
    {"incompatible_type: field ~s with type ~p cannot be compared "
     "to type ~p in where clause.", [Field, Expected, Actual]};
syntax_error_to_msg2({incompatible_operator, Field, ColType, Op}) ->
    {"incompatible_operator: field ~s with type ~p cannot use "
     "operator ~p in where clause.", [Field, ColType, Op]};
syntax_error_to_msg2({unexpected_where_field, Field}) ->
    {"unexpected_where_field: unexpected field ~s in where clause.",
     [Field]};
syntax_error_to_msg2({unexpected_select_field, Field}) ->
    {"unexpected_select_field: unexpected field ~s in select clause.",
     [Field]};
syntax_error_to_msg2({subexpressions_not_supported, Field, Op}) ->
    {"subexpressions_not_supported: expressions in where clause operators"
     " (~s ~s ...) are not supported.",
     [Field, Op]};
syntax_error_to_msg2({unknown_column_type, Other}) ->
    {"Unexpected select column type ~p.", [Other]}.


-spec is_query_valid(module(), #ddl_v1{}, #riak_sql_v1{}) ->
        true | {false, [query_syntax_error()]}.
is_query_valid(_, #ddl_v1{ table = T1 },
               #riak_sql_v1{ 'FROM' = T2 }) when T1 =/= T2 ->
    {false, [{bucket_type_mismatch, {T1, T2}}]};
is_query_valid(Mod, _,
               #riak_sql_v1{'SELECT' = #riak_sel_clause_v1{clause = Selection},
                            'WHERE'  = Where}) ->
    ValidSelection = are_selections_valid(Mod, Selection, ?CANTBEBLANK),
    ValidFilters   = check_filters_valid(Mod, Where),
    is_query_valid_result(ValidSelection, ValidFilters).

%%
is_query_valid_result(true,        true)        -> true;
is_query_valid_result(true,        {false, L})  -> {false, L};
is_query_valid_result({false, L},  true)        -> {false, L};
is_query_valid_result({false, L1}, {false, L2}) -> {false, L1 ++ L2}.

-spec check_filters_valid(module(), [filter()]) -> true | {false, [query_syntax_error()]}.
check_filters_valid(Mod, Where) ->
    Errors = fold_where_tree(Where, [],
        fun(Clause, Acc) ->
            is_filters_field_valid(Mod, Clause, Acc)
        end),
    case Errors of
        [] -> true;
        _  -> {false, Errors}
    end.

%% the terminal case of "a = 2"
is_filters_field_valid(Mod, {Op, Field, {RHS_type, RHS_Val}}, Acc1) ->
    case Mod:is_field_valid([Field]) of
        true  ->
            ExpectedType = Mod:get_field_type([Field]),
            case is_compatible_type(ExpectedType, RHS_type, normalise(RHS_Val)) of
                true  -> Acc2 = Acc1;
                false -> Acc2 = [{incompatible_type, Field, ExpectedType, RHS_type} | Acc1]
            end,
            case is_compatible_operator(Op, ExpectedType, RHS_type) of
                true  -> Acc2;
                false -> [{incompatible_operator, Field, ExpectedType, Op} | Acc2]
            end;
        false ->
            [{unexpected_where_field, Field} | Acc1]
    end;
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
-spec is_compatible_type(ColType::atom(), WhereType::atom(), any()) ->
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
                                 ExpectedType::simple_field_type(),
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
    case lists:foldl(CheckFn, {[], true}, Selections) of
        {[],   true}  -> true;
        {Msgs, false} -> {false, lists:reverse(Msgs)}
    end.

%% Reported error types must be supported by the function syntax_error_to_msg2 
is_selection_column_valid(Mod, {identifier, X}, {Acc, Status}) ->
    case Mod:is_field_valid(X) of
        true  ->
            {Acc, Status};
        false ->
            Msg = {unexpected_select_field, hd(X)},
            {[Msg | Acc], false}
    end;
is_selection_column_valid(Mod, {{window_agg_fn, Fn}, Args}, {Acc, Status}) ->
    % if the field is not an identifier, it should already be validated
    {Arity, FnTypeSig} = riak_ql_window_agg_fns:get_arity_and_type_sig(Fn),
    case length(Args) of
        Arity -> ExprTypes = type(Args, Mod, []),
                 case do_types_match(FnTypeSig, ExprTypes) of
                     true  -> {Acc, Status};
                     false -> Msg1 = {type_check_failed, Fn, Arity, ExprTypes},
                              {[Msg1 | Acc], false}
                 end;
        N     -> Msg2 = {fn_called_with_wrong_arity, Fn, Arity, N},
                 {[Msg2 | Acc], false}
    end;
is_selection_column_valid(_, {Type, _}, Acc) when is_atom(Type) ->
    % literal types, integer double etc.
    Acc;
is_selection_column_valid(_, {Op, _, _}, Acc) when is_atom(Op) ->
    % arithmetic
    Acc;
is_selection_column_valid(_, Other, {Acc, _}) ->
    {[{unknown_column_type, Other} | Acc], false}.

do_types_match(FnTypeSig, ExprTypes) ->
    CheckFn = fun(X, Acc) ->
                      case lists:keymember(X, 1, FnTypeSig) of
                          true  -> Acc;
                          false -> false
                      end
              end,
    lists:foldl(CheckFn, true, ExprTypes).

type([], _Mod, Acc) ->
    lists:reverse(Acc);
type([{identifier, I} | T], Mod, Acc) ->
    NewAcc = Mod:get_field_type(I),
    type(T, Mod, [NewAcc | Acc]).

%% Fold over the syntax tree for a where clause.
fold_where_tree([], Acc, _) ->
    Acc;
fold_where_tree([Where], Acc1, Fn) ->
    fold_where_tree(Where, Acc1, Fn);
fold_where_tree({Op, LHS, RHS}, Acc1, Fn) when Op == and_; Op == or_ ->
    Acc2 = fold_where_tree(LHS, Acc1, Fn),
    fold_where_tree(RHS, Acc2, Fn);
fold_where_tree(Clause, Acc, Fn) ->
    Fn(Clause, Acc).

-ifdef(TEST).
-compile(export_all).

-define(VALID,   true).
-define(INVALID, false).

-include_lib("eunit/include/eunit.hrl").

make_module_name_test() ->
    ?assertEqual('riak_ql_table_fafa$1', make_module_name(<<"fafa">>)),
    ?assertEqual('riak_ql_table_fafa$2', make_module_name(<<"fafa">>, 2)),
    ?assertEqual('riak_ql_table_FaFa$1', make_module_name(<<"FaFa">>, 1)),
    ?assertEqual('riak_ql_table_Fa%32%94%36$43', make_module_name(<<"Fa ^$">>, 43)).

%%
%% Helper Fn for unit tests
%%

mock_partition_fn(_A, _B, _C) -> mock_result.

make_ddl(Table, Fields) when is_binary(Table) ->
    make_ddl(Table, Fields, #key_v1{}, #key_v1{}).

make_ddl(Table, Fields, PK) when is_binary(Table) ->
    make_ddl(Table, Fields, PK, #key_v1{}).

make_ddl(Table, Fields, #key_v1{} = PK, #key_v1{} = LK)
  when is_binary(Table) ->
    #ddl_v1{table         = Table,
            fields        = Fields,
            partition_key = PK,
            local_key     = LK}.

%%
%% get partition_key tests
%%

simplest_partition_key_test() ->
    Name = <<"yando">>,
    PK = #key_v1{ast = [
                        #param_v1{name = [Name]}
                       ]},
    DDL = make_ddl(<<"simplest_partition_key_test">>,
                   [
                    #riak_field_v1{name     = Name,
                                   position = 1,
                                   type     = varchar}
                   ],
                   PK),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Obj = {Name},
    Result = (catch get_partition_key(DDL, Obj)),
    ?assertEqual([{varchar, Name}], Result).

simple_partition_key_test() ->
    Name1 = <<"yando">>,
    Name2 = <<"buckle">>,
    PK = #key_v1{ast = [
                        #param_v1{name = [Name1]},
                        #param_v1{name = [Name2]}
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
                   PK),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Obj = {<<"one">>, <<"two">>, <<"three">>},
    Result = (catch get_partition_key(DDL, Obj)),
    ?assertEqual([{varchar, <<"three">>}, {varchar, <<"one">>}], Result).

function_partition_key_test() ->
    Name1 = <<"yando">>,
    Name2 = <<"buckle">>,
    PK = #key_v1{ast = [
                        #param_v1{name = [Name1]},
                        #hash_fn_v1{mod  = ?MODULE,
                                    fn   = mock_partition_fn,
                                    args = [
                                            #param_v1{name = [Name2]},
                                            15,
                                            m
                                           ],
                                    type = timestamp
                                   }
                       ]},
    DDL = make_ddl(<<"function_partition_key_test">>,
                   [
                    #riak_field_v1{name     = Name2,
                                   position = 1,
                                   type     = timestamp},
                    #riak_field_v1{name     = <<"sherk">>,
                                   position = 2,
                                   type     = varchar},
                    #riak_field_v1{name     = Name1,
                                   position = 3,
                                   type     = varchar}
                   ],
                   PK),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Obj = {1234567890, <<"two">>, <<"three">>},
    Result = (catch get_partition_key(DDL, Obj)),
    %% Yes the mock partition function is actually computed
    %% read the actual code, lol
    Expected = [{varchar, <<"three">>}, {timestamp, mock_result}],
    ?assertEqual(Expected, Result).

complex_partition_key_test() ->
    Name0 = <<"yerp">>,
    Name1 = <<"yando">>,
    Name2 = <<"buckle">>,
    Name3 = <<"doodle">>,
    PK = #key_v1{ast = [
                        #param_v1{name = [Name0, Name1]},
                        #hash_fn_v1{mod  = ?MODULE,
                                    fn   = mock_partition_fn,
                                    args = [
                                            #param_v1{name = [
                                                              Name0,
                                                              Name2,
                                                              Name3
                                                             ]},
                                            "something",
                                            pong
                                           ],
                                    type = poodle
                                   },
                        #hash_fn_v1{mod  = ?MODULE,
                                    fn   = mock_partition_fn,
                                    args = [
                                            #param_v1{name = [
                                                              Name0,
                                                              Name1
                                                             ]},
                                            #param_v1{name = [
                                                              Name0,
                                                              Name2,
                                                              Name3
                                                             ]},
                                            pang
                                           ],
                                    type = wombat
                                   }
                       ]},
    Map3 = {map, [
                  #riak_field_v1{name     = <<"in_Map_2">>,
                                 position = 1,
                                 type     = sint64}
                 ]},
    Map2 = {map, [
                  #riak_field_v1{name     = Name3,
                                 position = 1,
                                 type     = sint64}
                 ]},
    Map1 = {map, [
                  #riak_field_v1{name     = Name1,
                                 position = 1,
                                 type     = sint64},
                  #riak_field_v1{name     = Name2,
                                 position = 2,
                                 type     = Map2},
                  #riak_field_v1{name     = <<"Level_1_map2">>,
                                 position = 3,
                                 type     = Map3}
                 ]},
    DDL = make_ddl(<<"complex_partition_key_test">>,
                   [
                    #riak_field_v1{name     = Name0,
                                   position = 1,
                                   type     = Map1}
                   ],
                   PK),
    {module, _Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Obj = {{2, {3}, {4}}},
    Result = (catch get_partition_key(DDL, Obj)),
    Expected = [{sint64, 2}, {poodle, mock_result}, {wombat, mock_result}],
    ?assertEqual(Expected, Result).

%%
%% get local_key tests
%%

local_key_test() ->
    Name = <<"yando">>,
    PK = #key_v1{ast = [
                        #param_v1{name = [Name]}
                       ]},
    LK = #key_v1{ast = [
                        #param_v1{name = [Name]}
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
%% Maps
%%

simple_valid_map_get_type_1_test() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yarple">>,
                                position = 1,
                                type     = sint64}
                ]},
    DDL = make_ddl(<<"simple_valid_map_get_type_1_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = Map},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Result = (catch Module:get_field_type([<<"erko">>, <<"yarple">>])),
    ?assertEqual(sint64, Result).

simple_valid_map_get_type_2_test() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yarple">>,
                                position = 1,
                                type     = sint64}
                ]},
    DDL = make_ddl(<<"simple_valid_map_get_type_2_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = Map},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Result = (catch Module:get_field_type([<<"erko">>])),
    ?assertEqual(map, Result).

complex_valid_map_get_type_test() ->
    Map3 = {map, [
                  #riak_field_v1{name     = <<"in_Map_2">>,
                                 position = 1,
                                 type     = sint64}
                 ]},
    Map2 = {map, [
                  #riak_field_v1{name     = <<"in_Map_1">>,
                                 position = 1,
                                 type     = sint64}
                 ]},
    Map1 = {map, [
                  #riak_field_v1{name     = <<"Level_1_1">>,
                                 position = 1,
                                 type     = sint64},
                  #riak_field_v1{name     = <<"Level_1_map1">>,
                                 position = 2,
                                 type     = Map2},
                  #riak_field_v1{name     = <<"Level_1_map2">>,
                                 position = 3,
                                 type     = Map3}
                 ]},
    DDL = make_ddl(<<"complex_valid_map_get_type_test">>,
                   [
                    #riak_field_v1{name     = <<"Top_Map">>,
                                   position = 1,
                                   type     = Map1}
                   ]),
    {module, Module} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Path = [<<"Top_Map">>, <<"Level_1_map1">>, <<"in_Map_1">>],
    Res = (catch Module:get_field_type(Path)),
    ?assertEqual(sint64, Res).

%%
%% make_key tests
%%

make_plain_key_test() ->
    Key = #key_v1{ast = [
                         #param_v1{name = [<<"user">>]},
                         #param_v1{name = [<<"time">>]}
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
    Key = #key_v1{ast = [
                         #param_v1{name = [<<"user">>]},
                         #hash_fn_v1{mod  = ?MODULE,
                                     fn   = mock_partition_fn,
                                     args = [
                                             #param_v1{name = [<<"time">>]},
                                             15,
                                             m
                                            ],
                                     type = timestamp
                                    }
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
    Expected = [{varchar, <<"user_1">>}, {timestamp, mock_result}],
    ?assertEqual(Expected, Got).

%%
%% Validate Query Tests
%%

partial_wildcard_are_selections_valid_test() ->
    Selections  = [{identifier, [<<"*">>]}],
    DDL = make_ddl(<<"partial_wildcard_are_selections_valid_test">>,
                   [
                    #riak_field_v1{name     = <<"temperature">>,
                                   position = 1,
                                   type     = sint64},
                    #riak_field_v1{name     = <<"geohash">>,
                                   position = 2,
                                   type     = sint64}
                   ]),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    ?assertEqual(
        true,
        are_selections_valid(Mod, Selections, ?CANTBEBLANK)
    ).

% FIXME this cannot happen because SQL without selections cannot be lexed
partial_are_selections_valid_fail_test() ->
    Selections  = [],
    DDL = make_ddl(<<"partial_are_selections_valid_fail_test">>,
                   [
                    #riak_field_v1{name     = <<"temperature">>,
                                   position = 1,
                                   type     = sint64},
                    #riak_field_v1{name     = <<"geohash">>,
                                   position = 2,
                                   type     = sint64}
                   ]),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    ?assertEqual(
        {false, [{selections_cant_be_blank, []}]},
        are_selections_valid(Mod, Selections, ?CANTBEBLANK)
    ).

%%
%% Query Validation tests
%%

simple_is_query_valid_test() ->
    Bucket = <<"simple_is_query_valid_test">>,
    Selections  = [{identifier, [<<"temperature">>]}, {identifier, [<<"geohash">>]}],
    Query = #riak_sql_v1{'FROM'   = Bucket,
                         'SELECT' = #riak_sel_clause_v1{clause = Selections}},
    DDL = make_ddl(Bucket,
                   [
                    #riak_field_v1{name     = <<"temperature">>,
                                   position = 1,
                                   type     = sint64},
                    #riak_field_v1{name     = <<"geohash">>,
                                   position = 2,
                                   type     = sint64}
                   ]),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    ?assertEqual(
        true,
        riak_ql_ddl:is_query_valid(Mod, DDL, Query)
    ).

simple_is_query_valid_map_test() ->
    Bucket = <<"simple_is_query_valid_map_test">>,
    Name0 = <<"name">>,
    Name1 = <<"temp">>,
    Name2 = <<"geo">>,
    Selections  = [{identifier, [<<"temp">>, <<"geo">>]},
                                      {identifier, [<<"name">>]}],
    Query = #riak_sql_v1{'FROM'   = Bucket,
                         'SELECT' = #riak_sel_clause_v1{clause = Selections}},
    Map = {map, [
                 #riak_field_v1{name     = Name2,
                                position = 1,
                                type     = sint64}
                ]},
    DDL = make_ddl(Bucket,
                   [
                    #riak_field_v1{name     = Name0,
                                   position = 1,
                                   type     = sint64},
                    #riak_field_v1{name     = Name1,
                                   position = 2,
                                   type     = Map}
                   ]),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    ?assertEqual(
        true,
        riak_ql_ddl:is_query_valid(Mod, DDL, Query)
    ).

simple_is_query_valid_map_wildcard_test() ->
    Bucket = <<"simple_is_query_valid_map_wildcard_test">>,
    Name0 = <<"name">>,
    Name1 = <<"temp">>,
    Name2 = <<"geo">>,
    Selections  = [{identifier, [<<"temp">>, <<"*">>]}, {identifier, [<<"name">>]}],
    Query = #riak_sql_v1{'FROM'   = Bucket,
                         'SELECT' = #riak_sel_clause_v1{clause = Selections}},
    Map = {map, [
                 #riak_field_v1{name     = Name2,
                                position = 1,
                                type     = sint64}
                ]},
    DDL = make_ddl(Bucket,
                   [
                    #riak_field_v1{name     = Name0,
                                   position = 1,
                                   type     = sint64},
                    #riak_field_v1{name     = Name1,
                                   position = 2,
                                   type     = Map}
                   ]),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
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
    Query = #riak_sql_v1{'FROM'   = Bucket,
                         'SELECT' = #riak_sel_clause_v1{clause = Selections},
                         'WHERE'  = Where},
    DDL = make_ddl(Bucket,
                   [
                    #riak_field_v1{name     = <<"temperature">>,
                                   position = 1,
                                   type     = sint64},
                    #riak_field_v1{name     = <<"geohash">>,
                                   position = 2,
                                   type     = sint64}
                   ]),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Res = riak_ql_ddl:is_query_valid(Mod, DDL, Query),
    ?assertEqual(true, Res).

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
    Query = #riak_sql_v1{'FROM'   = Bucket,
                         'SELECT' = #riak_sel_clause_v1{clause = Selections},
                         'WHERE'  = Where},
    DDL = make_ddl(Bucket,
                   [
                    #riak_field_v1{name     = <<"temperature">>,
                                   position = 1,
                                   type     = sint64},
                    #riak_field_v1{name     = <<"ne field">>,
                                   position = 2,
                                   type     = sint64},
                    #riak_field_v1{name     = <<"lte field">>,
                                   position = 3,
                                   type     = sint64},
                    #riak_field_v1{name     = <<"gte field">>,
                                   position = 4,
                                   type     = sint64}
                   ]),
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Res = riak_ql_ddl:is_query_valid(Mod, DDL, Query),
    ?assertEqual(true, Res).


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
    Query = #riak_sql_v1{'FROM'   = Bucket,
                         'SELECT' = #riak_sel_clause_v1{clause = Selections},
                         'WHERE'  = Where},
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
                                            #param_v1{name = [<<"time">>]},
                                            15,
                                            s
                                           ]}
                       ]},
    LK = #key_v1{ast = [
                        #param_v1{name = [<<"time">>]},
                        #param_v1{name = [<<"user">>]}]
                },
    DDL = #ddl_v1{table         = <<"timeseries_filter_test">>,
                  fields        = Fields,
                  partition_key = PK,
                  local_key     = LK
                 },
    {module, Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    Res = riak_ql_ddl:is_query_valid(Mod, DDL, Query),
    Expected = true,
    ?assertEqual(Expected, Res).

test_parse(SQL) ->
    element(2,
        riak_ql_parser:parse(
            riak_ql_lexer:get_tokens(SQL))).

is_query_valid_test_helper(Table_name, Table_def, Query) ->
    Mod_name = make_module_name(iolist_to_binary(Table_name)),
    catch code:purge(Mod_name),
    catch code:purge(Mod_name),
    DDL = test_parse(Table_def),
    % ?debugFmt("QUERY is ~p", [test_parse(Query)]),
    {module,Mod} = riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL),
    is_query_valid(Mod, DDL, test_parse(Query)).

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

fold_where_tree_test() ->
    #riak_sql_v1{ 'WHERE' = [Where] } = test_parse(
        "SELECT * FROM mytab "
        "WHERE time > 1 AND time < 10 "
        "AND myfamily = 'family1' "
        "AND myseries = 10 "),
    ?assertEqual(
        [<<"myseries">>, <<"myfamily">>, <<"time">>, <<"time">>],
        lists:reverse(fold_where_tree(Where, [],
                fun({_, Field, _}, Acc) -> [Field | Acc] end))
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
               #riak_sql_v1{'SELECT' = #riak_sel_clause_v1{clause = Selections}} = Q,
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

?select_test(simple_agg_fn_select_fail_1_test, "count(mysint64), avg(myvarchar)",
            {false, [
                     {type_check_failed, 'AVG', 1, [varchar]}
                    ]
            }).

?select_test(simple_agg_fn_select_fail_2_test, "count(mysint64, myboolean), avg(mysint64)",
            {false, [
                     {fn_called_with_wrong_arity, 'COUNT', 1, 2}
                    ]
            }).


-endif.
