%% -*- erlang -*-
%%% @doc       Parser for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2015 Basho

Nonterminals

Statement
Query
Select
Bucket
Buckets
Field
Fields
Identifier
CharacterLiteral
Where
Cond
Conds
Comp
Logic
Val
Vals
ArithOp
Funcall
TableDefinition
TableContentsSource
TableElementList
TableElements
TableElement
ColumnDefinition
ColumnConstraint
KeyDefinition
DataType
KeyFieldList
KeyField
KeyFieldArgList
KeyFieldArg
NotNull
CreateTable
PrimaryKey
FunArg
FunArgN
.

Terminals

or_
and_
boolean
character_literal
closeb
comma
create
datetime
div_
double
eq
false
float
from
gt
gte
identifier
integer
key
limit
lt
lte
maybetimes
minus
nomatch
not_
null
openb
plus
primary
quantum
regex
select
sint64
table
timestamp
true
varchar
where
.

Rootsymbol Statement.
Endsymbol '$end'.

Statement -> Query           : convert('$1').
Statement -> TableDefinition : fix_up_keys('$1').

Query -> Select limit integer : add_limit('$1', '$2', '$3').
Query -> Select               : '$1'.

Select -> select Fields from Buckets Where : make_clause('$1', '$2', '$3', '$4', '$5').
Select -> select Fields from Buckets       : make_clause('$1', '$2', '$3', '$4').

Where -> where Conds : make_where('$1', '$2').

ArithOp -> plus       : '$1'.
ArithOp -> minus      : '$1'.
ArithOp -> maybetimes : '$1'.
ArithOp -> div_       : '$1'.

Fields -> Fields ArithOp Field : make_select('$1', '$3').
Fields -> Fields comma   Field : make_select('$1', '$3').
Fields -> Field                : '$1'.

Field -> Identifier : make_select_type('$1').
Field -> maybetimes : make_select_type('$1').
Field -> Funcall    : make_select_type('$1').

Buckets -> Buckets comma Bucket : make_list('$1', '$3').
Buckets -> Bucket               : '$1'.

Bucket -> Identifier   : '$1'.

Identifier -> identifier : '$1'.

CharacterLiteral -> character_literal : character_literal_to_binary('$1').

FunArg -> Identifier : '$1'.
FunArg -> Val        : '$1'.
FunArg -> Funcall    : '$1'.

FunArgN -> comma FunArg  : '$1'.
FunArgN -> comma FunArg FunArgN : '$1'.

Funcall -> Identifier openb                closeb : make_funcall('$1', []).
Funcall -> Identifier openb FunArg         closeb : make_funcall('$1', ['$3']).
Funcall -> Identifier openb FunArg FunArgN closeb : make_funcall('$1', ['$3', '$4']).

Conds -> openb Conds closeb             : make_expr('$2').
Conds -> Conds Logic Cond               : make_expr('$1', '$2', '$3').
Conds -> Conds Logic openb Conds closeb : make_expr('$1', '$2', '$4').
Conds -> Cond                           : '$1'.

Cond -> Vals Comp Vals : make_expr('$1', '$2', '$3').

Vals -> Vals ArithOp Val : make_expr('$1', '$2', '$3').
Vals -> regex            : '$1'.
Vals -> Val              : '$1'.
Vals -> Funcall          : '$1'.
Vals -> Identifier       : '$1'.

Val -> integer identifier : add_unit('$1', '$2').
Val -> integer       : '$1'.
Val -> float     : '$1'.
Val -> datetime  : '$1'.
Val -> varchar   : '$1'.
Val -> CharacterLiteral : '$1'.
Val -> true : {boolean, true}.
Val -> false : {boolean, false}.

Logic -> and_ : '$1'.
Logic -> or_  : '$1'.

%% Comp -> approx    : '$1'.
Comp -> eq        : '$1'.
Comp -> gt        : '$1'.
Comp -> lt        : '$1'.
Comp -> gte       : '$1'.
Comp -> lte       : '$1'.
%% Comp -> ne        : '$1'.
Comp -> nomatch   : '$1'.
%% Comp -> notapprox : '$1'.

CreateTable -> create table : create_table.

NotNull -> not_ null : '$1'.

%% TABLE DEFINTITION

TableDefinition ->
    CreateTable Bucket TableContentsSource :
        make_table_definition('$2', '$3').

TableContentsSource -> TableElementList : '$1'.
TableElementList -> openb TableElements closeb : '$2'.

TableElements ->
    TableElement comma TableElements : make_table_element_list('$1', '$3').
TableElements -> TableElement : '$1'.

TableElement -> ColumnDefinition : '$1'.
TableElement -> KeyDefinition : '$1'.

ColumnDefinition ->
    Identifier DataType ColumnConstraint : make_column('$1', '$2', '$3').
ColumnDefinition ->
    Identifier DataType : make_column('$1', '$2').

ColumnConstraint -> NotNull : not_null.

DataType -> datetime  : '$1'.
DataType -> double    : '$1'.
DataType -> sint64    : '$1'.
DataType -> timestamp : '$1'.
DataType -> varchar   : '$1'.
DataType -> boolean   : '$1'.

PrimaryKey -> primary key : primary_key.

KeyDefinition ->
    PrimaryKey openb KeyFieldList closeb : make_local_key('$3').
KeyDefinition ->
    PrimaryKey openb openb KeyFieldList closeb comma KeyFieldList closeb : make_partition_and_local_keys('$4', '$7').

KeyFieldList -> KeyField comma KeyFieldList : make_list('$3', '$1').
KeyFieldList -> KeyField : make_list({list, []}, '$1').

KeyField -> quantum openb KeyFieldArgList closeb : make_modfun(quantum, '$3').
KeyField -> Identifier : '$1'.

KeyFieldArgList ->
    KeyFieldArg comma KeyFieldArgList : make_list('$3', '$1').
KeyFieldArgList ->
    KeyFieldArg : make_list({list, []}, '$1').

KeyFieldArg -> integer : '$1'.
KeyFieldArg -> float   : '$1'.
KeyFieldArg -> CharacterLiteral    : '$1'.
KeyFieldArg -> Identifier : '$1'.
%% KeyFieldArg -> atom openb Word closeb : make_atom('$3').

Erlang code.

-record(outputs,
        {
          type :: select | create,
          buckets = [],
          fields  = [],
          limit   = [],
          where   = [],
          ops     = []
         }).

-include("riak_ql_ddl.hrl").

%% export the return value function to prevent xref errors
%% this fun is used during the parsing and is marked as
%% unused/but not to be exported in the yecc source
%% no way to stop rebar borking on it AFAIK
-export([
         return_error/2
         ]).

-ifdef(TEST).
-include("riak_ql.yrl.tests").
-endif.

%% if no partition key is specified hash on the local key
fix_up_keys(#ddl_v1{partition_key = none, local_key = LK} = DDL) ->
    DDL#ddl_v1{partition_key = LK, local_key = LK};
fix_up_keys(A) ->
    A.

convert(#outputs{type    = select,
                 buckets = B,
                 fields  = F,
                 limit   = L,
                 where   = W}) ->
    Q = case B of
            {Type, _} when Type =:= list orelse Type =:= regex ->
                #riak_sql_v1{'SELECT' = F,
                             'FROM'   = B,
                             'WHERE'  = W,
                             'LIMIT'  = L};
            _ ->
                #riak_sql_v1{'SELECT'   = F,
                             'FROM'     = B,
                             'WHERE'    = W,
                             'LIMIT'    = L,
                             helper_mod = riak_ql_ddl:make_module_name(B)}
        end,
    Q;
convert(#outputs{type = create} = O) ->
    O.

%% make_atom({binary, SomeWord}) ->
%%     {atom, binary_to_atom(SomeWord, utf8)}.

make_clause(A, B, C, D) -> make_clause(A, B, C, D, {where, []}).

make_clause({select, _SelectBytes},
            {SelectType, B},
            {from, _FromBytes},
            {Type, D},
            {_WhereType, E}) ->
    Bucket = case Type of
                 identifier -> D;
                 list   -> {list, [X || X <- D]};
                 regex  -> {regex, D}
             end,
    Fields = case SelectType of
                 plain_row_select -> {plain_row_select, [[X] || X <- B]};
                 _Other           -> {SelectType, B}
             end,
    _O = #outputs{type    = select,
                  fields  = Fields,
                  buckets = Bucket,
                  where   = E
                 }.

add_limit(A, _B, {integer, C}) ->
    A#outputs{limit = C}.

make_expr({LiteralFlavor, Literal},
          {ComparisonType, _ComparisonBytes},
          {identifier, IdentifierName})
  when LiteralFlavor /= identifier ->
    FlippedComparison = flip_comparison(ComparisonType),
    make_expr({identifier, IdentifierName},
              {FlippedComparison, <<"flipped">>},
              {LiteralFlavor, Literal});
make_expr({identifier, _LeftIdentifier},
          {_ComparisonType, _ComparisonBin},
          {identifier, _RightIdentifier}) ->
    return_error(0, <<"Comparing or otherwise operating on two fields is not supported">>);
make_expr({_, A}, {B, _}, {Type, C}) ->
    B1 = case B of
             and_      -> and_;
             or_       -> or_;
             plus      -> '+';
             minus     -> '-';
             maybetime -> '*';
             div_      -> '/';
             gt        -> '>';
             lt        -> '<';
             gte       -> '>=';
             lte       -> '<=';
             eq        -> '=';
             ne        -> '<>';
             approx    -> '=~';
             notapprox -> '!~';
             nomatch   -> '!='
         end,
    C2 = case Type of
             conditional -> C;
             _           -> {Type, C}
         end,
    {conditional, {B1, A, C2}}.

make_where({where, A}, {conditional, B}) ->
    NewB = remove_conditionals(B),
    {A, [canonicalise(NewB)]}.

flip_comparison(lt) -> gt;
flip_comparison(gt) -> lt;
flip_comparison(lte) -> gte;
flip_comparison(gte) -> lte.

%%
%% rewrite the where clause to have a canonical form
%% makes query rewriting easier
%%
canonicalise(WhereClause) ->
    Canonical = canon2(WhereClause),
    _NewWhere = hoist(Canonical).

canon2({Cond, A, B}) when is_binary(B) andalso not is_binary(A) ->
    canonicalize_condition_order({Cond, B, A});
canon2({Cond, A, B}) when Cond =:= and_ orelse
                          Cond =:= or_  ->
    %% this is stack busting non-tail recursion
    %% but our where clauses are bounded in size so thats OK
    A1 = canon2(A),
    B1 = canon2(B),
    case is_lower(A1, B1) of
        true  -> {Cond, A1, B1};
        false -> {Cond, B1, A1}
    end;
canon2(A) ->
    A.

-spec canonicalize_condition_order({atom(), any(), binary()}) -> {atom(), binary(), any()}.
canonicalize_condition_order({'>', Reference, Column}) ->
    canon2({'<', Column, Reference});
canonicalize_condition_order({'<', Reference, Column}) ->
    canon2({'>', Column, Reference}).

hoist({and_, {and_, A, B}, C}) ->
    Hoisted = {and_, A, hoist({and_, B, C})},
    _Sort = sort(Hoisted);
hoist({A, B, C}) ->
    B2 = case B of
             {and_, _, _} -> hoist(B);
             _            -> B
         end,
    C2 = case C of
             {and_, _, _} -> hoist(C);
             _            -> C
         end,
    {A, B2, C2}.

%% a truly horendous bubble sort algo which is also
%% not tail recursive
sort({and_, A, {and_, B, C}}) ->
    case is_lower(A, B) of
        true  -> {and_, B1, C1} = sort({and_, B, C}),
                 case is_lower(A, B1) of
                     true  -> {and_, A, {and_, B1, C1}};
                     false -> sort({and_, B1, {and_, A, C1}})
                 end;
        false -> sort({and_, B, sort({and_, A, C})})
    end;
sort({Op, A, B}) ->
    case is_lower(A, B) of
        true  -> {Op, A, B};
        false -> {Op, B, A}
    end.

is_lower(Ands, {_, _, _}) when is_list(Ands)->
    true;
is_lower({_, _, _}, Ands) when is_list(Ands)->
    true;
is_lower(Ands1, Ands2) when is_list(Ands1) andalso is_list(Ands2) ->
    true;
is_lower({Op1, _, _} = A, {Op2, _, _} = B) when (Op1 =:= and_ orelse
                                         Op1 =:= or_  orelse
                                         Op1 =:= '>'  orelse
                                         Op1 =:= '<'  orelse
                                         Op1 =:= '>=' orelse
                                         Op1 =:= '<=' orelse
                                         Op1 =:= '='  orelse
                                         Op1 =:= '<>' orelse
                                         Op1 =:= '=~' orelse
                                         Op1 =:= '!~' orelse
                                         Op1 =:= '!=')
                                        andalso
                                        (Op2 =:= and_ orelse
                                         Op2 =:= or_  orelse
                                         Op2 =:= '>'  orelse
                                         Op2 =:= '<'  orelse
                                         Op2 =:= '>=' orelse
                                         Op2 =:= '<=' orelse
                                         Op2 =:= '='  orelse
                                         Op2 =:= '<>' orelse
                                         Op2 =:= '=~' orelse
                                         Op2 =:= '!~' orelse
                                         Op2 =:= '!=') ->
    (A < B).

remove_conditionals({conditional, A}) ->
    A;
remove_conditionals({A, B, C}) ->
    {A, remove_conditionals(B), remove_conditionals(C)};
remove_conditionals(A) ->
    A.

%% Functions are disabled so return an error.
make_funcall({identifier, FuncName}, Args) ->
    case get_func_type(FuncName) of
        window_fn ->
            Args2 = [#param_v1{name = X} || {identifier, X} <- Args],
            {funcall, #sql_window_fn_v1{fn   = list_to_atom(string:to_lower(binary_to_list(FuncName))),
                                        args = Args2}};
        not_supported ->
            Msg = io_lib:format("Function not supported - '~s'.", [FuncName]),
            return_error(0, iolist_to_binary(Msg))
    end;
make_funcall(_, _) ->
    % make dialyzer stop erroring on no local return.
    error.

get_func_type(FuncName) when is_binary(FuncName) ->
    case string:to_lower(binary_to_list(FuncName)) of
        "avg"   -> window_fn;
        "sum"   -> window_fn;
        "count" -> window_fn;
        "min"   -> window_fn;
        "max"   -> window_fn;
        "stdev" -> window_fn;
        _       -> not_supported
    end.

character_literal_to_binary({character_literal, CharacterLiteralBytes})
  when is_binary(CharacterLiteralBytes) ->
    {binary, CharacterLiteralBytes}.

add_unit({Type, A}, {identifier, U}) when U =:= <<"s">> -> {Type, A};
add_unit({Type, A}, {identifier, U}) when U =:= <<"m">> -> {Type, A*60};
add_unit({Type, A}, {identifier, U}) when U =:= <<"h">> -> {Type, A*60*60};
add_unit({Type, A}, {identifier, U}) when U =:= <<"d">> -> {Type, A*60*60*24};
add_unit({_, A}, {identifier, U}) ->
    return_error_flat(io_lib:format(
        "Used ~s as a measure of time in ~p~s. Only s, m, h and d are allowed.",
        [U, A, U]
    )).

%% log(Fn, Args) ->
%%     io:format("calling ~p for ~p~n", [Fn, Args]),
%%     exit(berk).

make_select_type({identifier, A}) -> {plain_row_select, [A]};
make_select_type({funcall   , A}) -> {window_select,    [A]};
make_select_type({maybetimes, A}) -> {plain_row_select, [A]}.

%% there are three different execution paths:
%% * window_select
%% * row_sel_with_arith
%% * plain_row_select
%% window_select subsumes row_select_with_arith subsumes plain_row_select
make_select({Type1, A}, {Type2, B}) when Type1 =:= window_select orelse
                                         Type2 =:= window_select ->
    {window_select, A ++ B};
make_select({Type1, A}, {Type2, B}) when Type1 =:= row_sel_with_arith orelse
                                         Type2 =:= row_sel_with_arith ->
    {row_sel_with_arith, A ++ B};
make_select({Type, A}, {_, B}) ->
    {Type, A ++ B}.

make_list({list, A}, {_, B}) ->
    {list, A ++ [B]};
make_list({_T1, A}, {_T2, B}) -> 
    {list, [A, B]}.

make_expr(A) ->
    {conditional, A}.

make_column({identifier, FieldName}, {DataType, _}) ->
    #riak_field_v1{
       name     = FieldName,
       type     = DataType,
       optional = true}.

make_column({identifier, FieldName}, {DataType, _}, not_null) ->
    #riak_field_v1{
       name     = FieldName,
       type     = DataType,
       optional = false}.

%% if only the local key is defined
%% use it as the partition key as well
make_local_key(FieldList) ->
    Key = #key_v1{ast = lists:reverse(extract_key_field_list(FieldList, []))},
    [
     {partition_key, Key},
     {local_key,     Key}
    ].

make_partition_and_local_keys(PFieldList, LFieldList) ->
    PFields = lists:reverse(extract_key_field_list(PFieldList, [])),
    LFields = lists:reverse(extract_key_field_list(LFieldList, [])),
    [
     {partition_key, #key_v1{ast = PFields}},
     {local_key,     #key_v1{ast = LFields}}
    ].

make_table_element_list(A, {table_element_list, B}) ->
    {table_element_list, [A] ++ lists:flatten(B)};
make_table_element_list(A, B) ->
    {table_element_list, lists:flatten([A, B])}.

extract_key_field_list({list, []}, Extracted) ->
    Extracted;
extract_key_field_list({list,
                        [Modfun = #hash_fn_v1{} | Rest]},
                       Extracted) ->
    [Modfun | extract_key_field_list({list, Rest}, Extracted)];
extract_key_field_list({list, [Field | Rest]}, Extracted) ->
    [#param_v1{name = [Field]} |
     extract_key_field_list({list, Rest}, Extracted)].

make_table_definition({identifier, Table}, Contents) ->
    validate_ddl(
      #ddl_v1{
         table = Table,
         partition_key = find_partition_key(Contents),
         local_key = find_local_key(Contents),
         fields = find_fields(Contents)}).

find_partition_key({table_element_list, Elements}) ->
    find_partition_key(Elements);
find_partition_key([{partition_key, Key} | _Rest]) ->
    Key;
find_partition_key([_Head | Rest]) ->
    find_partition_key(Rest);
find_partition_key(_) ->
    none.

find_local_key({table_element_list, Elements}) ->
    find_local_key(Elements);
find_local_key([{local_key, Key} | _Rest]) ->
    Key;
find_local_key([_Head | Rest]) ->
    find_local_key(Rest);
find_local_key(_) ->
    none.

make_modfun(quantum, {list, Args}) ->
    [Param, Quantity, Unit] = lists:reverse(Args),
    {modfun, #hash_fn_v1{
                mod  = riak_ql_quanta,
                fn   = quantum,
                args = [#param_v1{name = [Param]}, Quantity, binary_to_existing_atom(Unit, utf8)],
                type = timestamp
               }}.

find_fields({table_element_list, Elements}) ->
    find_fields(1, Elements, []).

find_fields(_Count, [], Found) ->
    lists:reverse(Found);
find_fields(Count, [Field = #riak_field_v1{} | Rest], Elements) ->
    PositionedField = Field#riak_field_v1{position = Count},
    find_fields(Count + 1, Rest, [PositionedField | Elements]);
find_fields(Count, [_Head | Rest], Elements) ->
    find_fields(Count, Rest, Elements).


%% DDL validation

validate_ddl(DDL) ->
    ok = assert_keys_present(DDL),
    ok = assert_unique_fields_in_pk(DDL),
    ok = assert_partition_key_length(DDL),
    ok = assert_primary_and_local_keys_match(DDL),
    ok = assert_partition_key_fields_exist(DDL),
    ok = assert_primary_key_fields_non_null(DDL),
    DDL.

%% @doc Ensure DDL can haz keys
assert_keys_present(#ddl_v1{local_key = LK, partition_key = PK})
  when LK == none;
       PK == none ->
    return_error_flat("Missing primary key");
assert_keys_present(_GoodDDL) ->
    ok.

%% @doc Ensure all fields appearing in PRIMARY KEY are not null.
assert_primary_key_fields_non_null(#ddl_v1{local_key = #key_v1{ast = LK},
                                           fields = Fields}) ->
    PKFieldNames = [N || #param_v1{name = [N]} <- LK],
    OnlyPKFields = [F || #riak_field_v1{name = N} = F <- Fields,
                         lists:member(N, PKFieldNames)],
    NonNullFields =
        [binary_to_list(F) || #riak_field_v1{name = F, optional = Null}
                                  <- OnlyPKFields, Null == true],
    case NonNullFields of
        [] ->
            ok;
        NonNullFields ->
            return_error_flat("Primary key has 'null' fields (~s)",
                              [string:join(NonNullFields, ", ")])
    end.

%% @doc Verify that the primary key has three components
%%      and the third element is a quantum
assert_partition_key_length(#ddl_v1{partition_key = {key_v1, Key}}) when length(Key) == 3 ->
    assert_param_is_quantum(lists:nth(3, Key));
assert_partition_key_length(#ddl_v1{partition_key = {key_v1, Key}}) ->
    return_error_flat("Primary key must consist of exactly 3 fields (has ~b)", [length(Key)]).

%% @doc Verify that the key element is a quantum
assert_param_is_quantum(#hash_fn_v1{mod = riak_ql_quanta, fn = quantum}) ->
    ok;
assert_param_is_quantum(_KeyComponent) ->
    return_error_flat("Third element of primary key must be a quantum").

%% @doc Verify primary key and local partition have the same elements
assert_primary_and_local_keys_match(#ddl_v1{partition_key = #key_v1{ast = Primary},
                                            local_key = #key_v1{ast = Local}}) ->
    PrimaryList = [query_field_name(F) || F <- Primary],
    LocalList = [query_field_name(F) || F <- Local],
    case PrimaryList == LocalList of
        true ->
            ok;
        false ->
            return_error_flat("Local key does not match primary key")
    end.

assert_unique_fields_in_pk(#ddl_v1{local_key = #key_v1{ast = LK}}) ->
    Fields = [N || #param_v1{name = [N]} <- LK],
    case length(Fields) == length(lists:usort(Fields)) of
        true ->
            ok;
        false ->
            return_error_flat(
              "Primary key has duplicate fields (~s)",
              [string:join(
                 which_duplicate(
                   lists:sort(
                     [binary_to_list(F) || F <- Fields])),
                 ", ")])
    end.



%% Ensure that all fields in the primary key exist in the table definition.
assert_partition_key_fields_exist(#ddl_v1{ fields = Fields,
                                           partition_key =
                                               #key_v1{ ast = PK } }) ->
    MissingFields =
        [binary_to_list(name_of(F)) || F <- PK, not is_field(F, Fields)],
    case MissingFields of
        [] ->
            ok;
        _ ->
            return_error_flat("Primary key fields do not exist (~s).",
                              [string:join(MissingFields, ", ")])
    end.

%% Check that the field name exists in the list of fields.
is_field(Field, Fields) ->
    (lists:keyfind(name_of(Field), 2, Fields) /= false).

%%
name_of(#param_v1{ name = [N] }) ->
    N;
name_of(#hash_fn_v1{ args = [#param_v1{ name = [N] }|_] }) ->
    N.

which_duplicate(FF) ->
    which_duplicate(FF, []).
which_duplicate([], Acc) ->
    Acc;
which_duplicate([_], Acc) ->
    Acc;
which_duplicate([A,A|_] = [_|T], Acc) ->
    which_duplicate(T, [A|Acc]);
which_duplicate([_|T], Acc) ->
    which_duplicate(T, Acc).

%% Pull the name out of the appropriate record
query_field_name(#hash_fn_v1{args = Args}) ->
    Param = lists:keyfind(param_v1, 1, Args),
    query_field_name(Param);
query_field_name(#param_v1{name = Field}) ->
    Field.

-spec return_error_flat(string()) -> no_return().
return_error_flat(F) ->
    return_error_flat(F, []).
-spec return_error_flat(string(), [term()]) -> no_return().
return_error_flat(F, A) ->
    return_error(
      0, iolist_to_binary(io_lib:format(F, A))).
