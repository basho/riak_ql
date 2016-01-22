%% -*- erlang -*-
%%% @doc       Parser for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2015, 2016 Basho

Nonterminals

Statement
StatementWithoutSemicolon
Query
Select
Describe
Bucket
Buckets
Field
FieldElem
Fields
Identifier
CharacterLiteral
Where
Cond
Comp
Val
Vals
Funcall
TableDefinition
TableContentsSource
TableElementList
TableElements
TableElement
TableProperties
TablePropertyList
TableProperty
TablePropertyValue
ColumnDefinition
ColumnConstraint
KeyDefinition
DataType
KeyFieldList
KeyField
KeyFieldArgList
KeyFieldArg
NotNull

%% ValueExpression
%% CommonValueExpression

NumericValueExpression
Term
Factor
NumericPrimary

BooleanValueExpression
BooleanTerm
BooleanFactor
BooleanTest
TruthValue
BooleanPrimary
BooleanPredicand

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
comma
create
describe
double
equals_operator
false
float
from
greater_than_operator
gte
identifier
integer
key
limit
left_paren
less_than_operator
lte
asterisk
minus_sign
nomatch
not_
null
plus_sign
primary
quantum
regex
right_paren
select
semicolon
sint64
solidus
table
timestamp
true
varchar
where
with
.

Rootsymbol Statement.
Endsymbol '$end'.

Statement -> StatementWithoutSemicolon : '$1'.
Statement -> StatementWithoutSemicolon semicolon : '$1'.

StatementWithoutSemicolon -> Query           : convert('$1').
StatementWithoutSemicolon -> TableDefinition : fix_up_keys('$1').
StatementWithoutSemicolon -> Describe : '$1'.

Query -> Select limit integer : add_limit('$1', '$2', '$3').
Query -> Select               : '$1'.

Select -> select Fields from Buckets Where : make_select('$1', '$2', '$3', '$4', '$5').
Select -> select Fields from Buckets       : make_select('$1', '$2', '$3', '$4').

%% 20.9 DESCRIBE STATEMENT
Describe -> describe Bucket : make_describe('$2').

Where -> where BooleanValueExpression : make_where('$1', '$2').

Fields -> Fields     comma   FieldElem   : concat_select('$1', '$3').
Fields -> FieldElem                      : '$1'.

FieldElem -> Field : '$1'.
FieldElem -> Val   : '$1'.

Field -> NumericValueExpression : '$1'.
%Field -> Identifier    : canonicalise_col('$1').
Field -> asterisk : make_wildcard('$1').

Buckets -> Buckets comma Bucket : make_list('$1', '$3').
Buckets -> Bucket               : '$1'.

Bucket -> Identifier   : '$1'.

Identifier -> identifier : '$1'.

CharacterLiteral -> character_literal : character_literal_to_binary('$1').

FunArg -> NumericValueExpression : '$1'.
FunArg -> Val        : '$1'.
%% FunArg -> Funcall    : '$1'.

FunArgN -> comma FunArg         : ['$2'].
FunArgN -> comma FunArg FunArgN : ['$2' , '$3'].

Funcall -> Identifier left_paren                right_paren : make_funcall('$1', []).
Funcall -> Identifier left_paren FunArg         right_paren : make_funcall('$1', ['$3']).
Funcall -> Identifier left_paren asterisk       right_paren : make_funcall('$1', ['$3']).
Funcall -> Identifier left_paren FunArg FunArgN right_paren : make_funcall('$1', ['$3'] ++ '$4').

Cond -> Vals Comp Vals : make_expr('$1', '$2', '$3').

Vals -> NumericValueExpression : '$1'.
Vals -> regex            : '$1'.
Vals -> Val              : '$1'.

Val -> varchar            : '$1'.
Val -> CharacterLiteral   : '$1'.
Val -> TruthValue         : '$1'.

%% Comp -> approx    : '$1'.
Comp -> equals_operator        : '$1'.
Comp -> greater_than_operator  : '$1'.
Comp -> less_than_operator     : '$1'.
Comp -> gte                    : '$1'.
Comp -> lte                    : '$1'.
%% Comp -> ne                  : '$1'.
Comp -> nomatch                : '$1'.
%% Comp -> notapprox           : '$1'.

CreateTable -> create table : create_table.

NotNull -> not_ null : '$1'.

%% %% 6.26 VALUE EXPRESSION

%% ValueExpression -> CommonValueExpression : '$1'.
%% ValueExpression -> BooleanValueExpression : '$1'.

%% CommonValueExpression ->
%%     NumericValueExpression : '$1'.
%% % todo: 6.29 string value expression
%% CommonValueExpression ->
%%     character_literal : '$1'.

%% 6.27 NUMERIC VALUE EXPRESSION

NumericValueExpression -> Term : '$1'.
NumericValueExpression ->
    NumericValueExpression plus_sign Term :
        make_expr('$1', '$2', '$3').
NumericValueExpression ->
    NumericValueExpression minus_sign Term :
        make_expr('$1', '$2', '$3').

Term -> Factor : '$1'.
Term ->
    Term asterisk Factor :
        make_expr('$1', '$2', '$3').
Term ->
    Term solidus Factor :
        make_expr('$1', '$2', '$3').

Factor -> NumericPrimary : '$1'.
Factor -> plus_sign NumericPrimary : '$2'.
Factor -> minus_sign NumericPrimary : {negate, '$2'}.

NumericPrimary -> integer identifier : add_unit('$1', '$2').
NumericPrimary -> integer : '$1'.
NumericPrimary -> float : '$1'.
NumericPrimary -> Identifier : '$1'.
NumericPrimary -> Funcall : '$1'.
NumericPrimary -> left_paren NumericValueExpression right_paren : '$2'.
% NumericPrimary -> NumericValueFunction : '$1'.

%% 6.35 BOOLEAN VALUE EXPRESSION

BooleanValueExpression -> BooleanTerm : '$1'.
BooleanValueExpression ->
    BooleanValueExpression or_ BooleanTerm :
        {expr, {or_, '$1', '$3'}}.

BooleanTerm -> BooleanFactor : '$1'.
BooleanTerm ->
    BooleanTerm and_ BooleanFactor :
        {expr, {and_, '$1', '$3'}}.

BooleanFactor -> BooleanTest : '$1'.
BooleanFactor -> not_ BooleanTest : {not_, '$1'}.

BooleanTest -> BooleanPrimary : '$1'.
%% i guess we don't have IS right now?
%% BooleanTest -> BooleanPrimary is TruthValue : {'=', '$1', '$3'}.
%% BooleanTest -> BooleanPrimary is not_ TruthValue : {'<>', '$1', '$4'}.

TruthValue -> true : {boolean, true}.
TruthValue -> false : {boolean, false}.

BooleanPrimary -> BooleanPredicand : '$1'.

BooleanPredicand ->
    Cond : '$1'.
BooleanPredicand ->
    left_paren BooleanValueExpression right_paren : '$2'.

%% TABLE DEFINTITION

TableDefinition ->
    CreateTable Bucket TableContentsSource :
        make_table_definition('$2', '$3').
TableDefinition ->
    CreateTable Bucket TableContentsSource with TableProperties :
        make_table_definition('$2', '$3', '$5').

TableContentsSource -> TableElementList : '$1'.
TableElementList -> left_paren TableElements right_paren : '$2'.

TableElements ->
    TableElement comma TableElements : make_table_element_list('$1', '$3').
TableElements -> TableElement        : make_table_element_list('$1').

TableElement -> ColumnDefinition : '$1'.
TableElement -> KeyDefinition : '$1'.

ColumnDefinition ->
    Identifier DataType ColumnConstraint : make_column('$1', '$2', '$3').
ColumnDefinition ->
    Identifier DataType : make_column('$1', '$2').

ColumnConstraint -> NotNull : not_null.

DataType -> double    : '$1'.
DataType -> sint64    : '$1'.
DataType -> timestamp : '$1'.
DataType -> varchar   : '$1'.
DataType -> boolean   : '$1'.

PrimaryKey -> primary key : primary_key.

KeyDefinition ->
    PrimaryKey left_paren KeyFieldList right_paren : make_local_key('$3').
KeyDefinition ->
    PrimaryKey left_paren left_paren KeyFieldList right_paren comma KeyFieldList right_paren : make_partition_and_local_keys('$4', '$7').

KeyFieldList -> KeyField comma KeyFieldList : make_list('$3', '$1').
KeyFieldList -> KeyField : make_list({list, []}, '$1').

KeyField -> quantum left_paren KeyFieldArgList right_paren : make_modfun(quantum, '$3').
KeyField -> Identifier : '$1'.

KeyFieldArgList ->
    KeyFieldArg comma KeyFieldArgList : make_list('$3', '$1').
KeyFieldArgList ->
    KeyFieldArg : make_list({list, []}, '$1').

KeyFieldArg -> integer : '$1'.
KeyFieldArg -> float   : '$1'.
KeyFieldArg -> CharacterLiteral    : '$1'.
KeyFieldArg -> Identifier : '$1'.
%% KeyFieldArg -> atom left_paren Word right_paren : make_atom('$3').

TableProperties ->
    left_paren TablePropertyList right_paren : '$2'.

TablePropertyList ->
    TableProperty : prepend_table_proplist([], '$1').
TablePropertyList ->
    TableProperty comma TablePropertyList : prepend_table_proplist('$3', '$1').

TableProperty ->
    identifier equals_operator TablePropertyValue :
        make_table_property('$1', '$3').

TablePropertyValue -> integer : '$1'.
TablePropertyValue -> character_literal : '$1'.

Erlang code.

-record(outputs,
        {
          type :: select | create | describe,
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
         return_error/2,
         canonicalise_where/1
         ]).

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
                ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = F},
                            'FROM'   = B,
                            'WHERE'  = W,
                            'LIMIT'  = L};
            _ ->
                ?SQL_SELECT{'SELECT'   = #riak_sel_clause_v1{clause = F},
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

make_select(A, B, C, D) -> make_select(A, B, C, D, {where, []}).

make_select({select, _SelectBytes},
            Select,
            {from, _FromBytes},
            {Type, D},
            {_WhereType, E}) ->
    Bucket = case Type of
                 identifier -> D;
                 list   -> {list, [X || X <- D]};
                 regex  -> {regex, D}
             end,
    FieldsAsList = case is_list(Select) of
                       true  -> Select;
                       false -> [Select]
                   end,
    FieldsWithoutExprs = [remove_exprs(X) || X <- FieldsAsList],
    FieldsWrappedIdentifiers = [wrap_identifier(X) || X <- FieldsWithoutExprs],
    _O = #outputs{type    = select,
                  fields  = FieldsWrappedIdentifiers,
                  buckets = Bucket,
                  where   = E
                 }.


wrap_identifier({identifier, IdentifierName})
  when is_binary(IdentifierName) ->
    {identifier, [IdentifierName]};
wrap_identifier(Default) -> Default.

make_describe({identifier, D}) ->
    #riak_sql_describe_v1{'DESCRIBE' = D}.

add_limit(A, _B, {integer, C}) ->
    A#outputs{limit = C}.

make_expr({LiteralFlavor, Literal},
          {Op, _},
          {identifier, IdentifierName}) when LiteralFlavor /= identifier ->
    % if the literal is on left hand side then rewrite the expression, putting
    % on the right, this means flipping greater than to less than and vice versa
    FlippedComparison = maybe_flip_op(Op),
    make_expr({identifier, IdentifierName},
              {FlippedComparison, <<"flipped">>},
              {LiteralFlavor, Literal});
make_expr({TypeA, A}, {B, _}, {Type, C}) ->
    B1 = case B of
             and_                   -> and_;
             or_                    -> or_;
             plus_sign              -> '+';
             minus_sign             -> '-';
             asterisk               -> '*';
             solidus                -> '/';
             greater_than_operator  -> '>';
             less_than_operator     -> '<';
             gte                    -> '>=';
             lte                    -> '<=';
             equals_operator        -> '=';
             ne                     -> '<>';
             approx                 -> '=~';
             notapprox              -> '!~';
             nomatch                -> '!='
         end,
    C2 = case Type of
             expr -> C;
             _    -> {Type, C}
         end,
    {expr, {B1, {TypeA, A}, C2}}.

make_wildcard({asterisk, <<"*">>}) -> {identifier, [<<"*">>]}.

make_where({where, A}, {expr, B}) ->
    NewB = remove_exprs(B),
    {A, [canonicalise_where(NewB)]}.

maybe_flip_op(less_than_operator)    -> greater_than_operator;
maybe_flip_op(greater_than_operator) -> less_than_operator;
maybe_flip_op(lte)                   -> gte;
maybe_flip_op(gte)                   -> lte;
maybe_flip_op(Op)                    -> Op.

%%
%% rewrite the where clause to have a canonical form
%% makes query rewriting easier
%%
canonicalise_where(WhereClause) ->
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
    case A1 == B1 of
        true ->
            A1;
        false ->
            case is_lower(A1, B1) of
                true  -> {Cond, A1, B1};
                false -> {Cond, B1, A1}
            end
    end;
canon2({Op, A, B}) ->
    {Op, strip(A), strip(B)};
canon2(A) ->
    A.

strip({identifier, A}) -> A;
strip(A)               -> A.

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
        true ->
            {and_, B1, C1} = sort({and_, B, C}),
            case is_lower(A, B1) of
                true  -> {and_, A, {and_, B1, C1}};
                false -> sort({and_, B1, {and_, A, C1}})
            end;
        false ->
            sort({and_, B, sort({and_, A, C})})
    end;
sort({and_, A, A}) ->
    A;
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
    (A =< B).

remove_exprs({expr, A}) ->
    remove_exprs(A);
remove_exprs({A, B, C}) ->
    {A, remove_exprs(B), remove_exprs(C)};
remove_exprs(A) ->
    A.

%% Functions are disabled so return an error.
make_funcall({identifier, FuncName}, Args) ->
    Fn = canonicalise_window_aggregate_fn(FuncName),
    case get_func_type(Fn) of
        window_aggregate_fn ->
            {Fn2, Args2} = case {Fn, Args} of
                               {'COUNT', [{asterisk, _Asterisk}]} ->
                                   {'COUNT', [{identifier, <<"*">>}]};
                               {_, [{asterisk, _Asterisk}]} ->
                                   Msg1 = io_lib:format("Function '~s' does not support"
                                                        " wild cards args.", [Fn]),
                                   return_error(0, iolist_to_binary(Msg1));
                               _ ->
                                   {Fn, Args}
                           end,
            Args3 = [canonicalise_expr(X) || X <- Args2],
            {{window_agg_fn, Fn2}, Args3};
        not_supported ->
            Msg2 = io_lib:format("Function not supported - '~s'.", [FuncName]),
            return_error(0, iolist_to_binary(Msg2))
    end;
make_funcall(_, _) ->
    % make dialyzer stop erroring on no local return.
    error.

canonicalise_expr({identifier, X}) ->
    {identifier, [X]};
canonicalise_expr({expr, X}) ->
    X.

get_func_type(FuncName) when FuncName =:= 'AVG'    orelse
                             FuncName =:= 'MEAN'   orelse
                             FuncName =:= 'SUM'    orelse
                             FuncName =:= 'COUNT'  orelse
                             FuncName =:= 'MIN'    orelse
                             FuncName =:= 'MAX'    orelse
                             FuncName =:= 'STDDEV' -> window_aggregate_fn;
get_func_type(FuncName) when is_atom(FuncName)     -> not_supported.

%% TODO
%% this list to atom needs to change to list to existing atom
%% once the fns that actually execute the Window_Aggregates Fns are written then the atoms
%% will definetely be existing - but just not now
%% also try/catch round it
canonicalise_window_aggregate_fn(Fn) when is_binary(Fn)->
     list_to_atom(string:to_upper(binary_to_list(Fn))).

%% canonicalise_col({identifier, X}) -> {identifier, [X]};
%% canonicalise_col(X)               -> X.

character_literal_to_binary({character_literal, CharacterLiteralBytes})
  when is_binary(CharacterLiteralBytes) ->
    {binary, CharacterLiteralBytes}.

%%
add_unit({Type, Value}, {identifier, Unit1}) ->
    Unit2 = list_to_binary(string:to_lower(binary_to_list(Unit1))),
    case riak_ql_quanta:unit_to_millis(Value, Unit2) of
        error ->
            return_error_flat(io_lib:format(
                "Used ~s as a measure of time in ~p~s. Only s, m, h and d are allowed.",
                [Unit2, Value, Unit2]
            ));
        Millis ->
            {Type, Millis}
    end.

concat_select(L1, L2) when is_list(L1) andalso
                           is_list(L2) ->
    L1 ++ L2;
concat_select(L1, El2) when is_list(L1) ->
    L1 ++ [El2];
concat_select(El1, El2) ->
    [El1, El2].

make_list({list, A}, {_, B}) ->
    {list, A ++ [B]};
make_list({_T1, A}, {_T2, B}) ->
    {list, [A, B]}.

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

make_table_element_list(A) ->
    {table_element_list, [A]}.

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

make_table_definition(TableName, Contents) ->
    make_table_definition(TableName, Contents, []).
make_table_definition({identifier, Table}, Contents, Properties) ->
    {validate_ddl(
       #ddl_v1{
          table = Table,
          partition_key = find_partition_key(Contents),
          local_key = find_local_key(Contents),
          fields = find_fields(Contents)}),
     validate_table_properties(Properties)}.

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

prepend_table_proplist(L, P) ->
    [P | L].

make_table_property({identifier, K}, {Type, V})
  when Type == integer;
       Type == character_literal ->
    {K, V}.

validate_table_properties(Properties) ->
    %% We let all k=v in: there's more substantial validation and
    %% enrichment happening in riak_kv_wm_utils:erlify_bucket_prop
    Properties.


%% DDL validation

validate_ddl(DDL) ->
    ok = assert_keys_present(DDL),
    ok = assert_unique_fields_in_pk(DDL),
    ok = assert_partition_key_length(DDL),
    ok = assert_primary_and_local_keys_match(DDL),
    ok = assert_partition_key_fields_exist(DDL),
    ok = assert_primary_key_fields_non_null(DDL),
    ok = assert_field_order_matches_key_order(DDL),
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

%% Ensure the keys appearing in the key follow the order in which they are defined
assert_field_order_matches_key_order(#ddl_v1{fields = Fields,
                                             local_key = #key_v1{ast = LK}}) ->
    KeyFieldsInOrder = [N || #param_v1{name = [N]} <- LK],
    OnlyKeyFields = [F || #riak_field_v1{name = F} <- Fields, lists:member(F, KeyFieldsInOrder)],
    case KeyFieldsInOrder == OnlyKeyFields of
        true ->
            ok;
        false ->
            return_error_flat("Keys must have fields in the order they appear in the table definition")
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
