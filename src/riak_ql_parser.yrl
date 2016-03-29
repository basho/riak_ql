%%% -*- erlang -*-
%%% @doc       Parser for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2016 Basho

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
Insert
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

OptFieldList
IdentifierList
RowValueList
RowValue
FieldValue
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
insert
integer
into
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
values
varchar
where
.

Rootsymbol Statement.
Endsymbol '$end'.

Statement -> StatementWithoutSemicolon           : '$1'.
Statement -> StatementWithoutSemicolon semicolon : '$1'.

StatementWithoutSemicolon -> Query           : '$1'.
StatementWithoutSemicolon -> TableDefinition : '$1'.
StatementWithoutSemicolon -> Describe        : '$1'.
StatementWithoutSemicolon -> Insert          : '$1'.

Query -> Select limit integer : {ql_select, '$1', '$3'}.
Query -> Select               : {ql_select, '$1', none}.

Select -> select Fields from Buckets Where : {'$2', '$4', '$5'}.
Select -> select Fields from Buckets       : {'$2', '$4', {where, []}}.

%% 20.9 DESCRIBE STATEMENT
Describe -> describe Bucket : {ql_describe, '$2'}.

Insert -> insert into Identifier OptFieldList values RowValueList : {ql_insert, '$3', '$4', '$6'}.

Where -> where BooleanValueExpression : {'$1', '$2'}.
%%riak_ql_where_pipeline:make_where('$1', '$2', emit_vsn()).

Fields -> Fields    comma   FieldElem   : concat_select('$1', '$3').
Fields -> FieldElem                     : '$1'.

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
    CreateTable Bucket TableContentsSource : {ql_create_table, '$2', '$3'}.

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


OptFieldList -> left_paren IdentifierList right_paren : '$2'.
OptFieldList -> '$empty' : undefined.

IdentifierList -> IdentifierList comma Identifier : '$1' ++ ['$3'].
IdentifierList -> Identifier : ['$1'].

RowValueList -> left_paren right_paren : [[]].
RowValueList -> left_paren RowValue right_paren : ['$2'].
RowValueList -> RowValueList comma left_paren RowValue right_paren : '$1' ++ ['$4'].

RowValue -> RowValue comma FieldValue : '$1' ++ ['$3'].
RowValue -> FieldValue : ['$1'].

FieldValue -> integer          : '$1'.
FieldValue -> float            : '$1'.
FieldValue -> CharacterLiteral : '$1'.
FieldValue -> Identifier       : '$1'.

Erlang code.

-include("riak_ql_ddl.hrl").

%% export the return value function to prevent xref errors
%% this fun is used during the parsing and is marked as
%% unused/but not to be exported in the yecc source
%% no way to stop rebar borking on it AFAIK
-export([
         return_error/2,
         return_error_flat/1,
         return_error_flat/2,
         ql_parse/1,
         get_version/0
        ]).

-export([
         parse_TEST/1,
         post_process_TEST/3
        ]).

get_version() -> 1.

parse_TEST(Tokens) -> parse(Tokens).

post_process_TEST(Result, Capability1, Capability2) -> 
    post_process(Result, Capability1, Capability2).

%% Provide more useful success tuples
ql_parse(Tokens) ->
    Result = parse(Tokens),
    CompilerCapability = riak_core_capability:get({query_pipeline, query_compiler}),
    QueryCapability = riak_core_capability:get({query_pipeline, query_coordinator}),
    post_process(Result, {query_compiler, CompilerCapability}, {query_coordinator, QueryCapability}).

post_process(Result, CompilerCapability, QueryCapability) ->
    case Result of
        {error, _} = Err ->
            Err;
        {ok, {ql_create_table, Table, Contents}} ->
            riak_ql_create_table_pipeline:make_create_table(Table, Contents);
        {ok, {ql_select, Details, Limit}} ->
            riak_ql_select_pipeline:make_select(Details, Limit, CompilerCapability, QueryCapability);
        {ok, {ql_describe, Table}} ->
            riak_ql_describe_pipeline:make_describe(Table);
        {ok, {ql_insert, Table, Field, Values}} ->
            riak_ql_insert_pipeline:make_insert(Table, Field, Values)
    end.

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


maybe_flip_op(less_than_operator)    -> greater_than_operator;
maybe_flip_op(greater_than_operator) -> less_than_operator;
maybe_flip_op(lte)                   -> gte;
maybe_flip_op(gte)                   -> lte;
maybe_flip_op(Op)                    -> Op.

%% Functions are disabled so return an error.
make_funcall({identifier, FuncName}, Args) ->
    Fn = canonicalise_window_aggregate_fn(FuncName),
    case get_func_type(Fn) of
        window_aggregate_fn ->
            %% FIXME this should be in the type checker in riak_kv_qry_compiler
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
    X;
canonicalise_expr(X) ->
    X.

get_func_type(FuncName) when FuncName =:= 'AVG'    orelse
                             FuncName =:= 'MEAN'   orelse
                             FuncName =:= 'SUM'    orelse
                             FuncName =:= 'COUNT'  orelse
                             FuncName =:= 'MIN'    orelse
                             FuncName =:= 'MAX'    orelse
                             FuncName =:= 'STDDEV' orelse
                             FuncName =:= 'STDDEV_SAMP' orelse
                             FuncName =:= 'STDDEV_POP' ->
    window_aggregate_fn;
get_func_type(FuncName) when is_atom(FuncName) ->
    not_supported.

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

make_modfun(quantum, {list, Args}) ->
    [Param, Quantity, Unit] = lists:reverse(Args),
    {modfun, #hash_fn_v1{
                mod  = riak_ql_quanta,
                fn   = quantum,
                args = [#param_v1{name = [Param]}, Quantity, binary_to_existing_atom(Unit, utf8)],
                type = timestamp
               }}.

-spec return_error_flat(string()) -> no_return().
return_error_flat(F) ->
    return_error_flat(F, []).
-spec return_error_flat(string(), [term()]) -> no_return().
return_error_flat(F, A) ->
    return_error(
      0, iolist_to_binary(io_lib:format(F, A))).
