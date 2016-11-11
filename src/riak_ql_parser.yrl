%% -*- erlang -*-
%%% @doc       Parser for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2016 Basho

Nonterminals

Bucket
CharacterLiteral
ColumnConstraint
ColumnDefinition
Comp
ComparisonPredicate
DataType
Describe
Explain
Field
FieldElem
Fields
Funcall
GroupBy
Identifier
Insert
IsNotNull
IsNull
KeyDefinition
KeyField
KeyFieldArg
KeyFieldArgList
KeyFieldList
LimitClause
NotNull
NullComp
NullOrderSpec
NullPredicate
OrderingSpec
Query
ResultOffsetClause
Select
ShowTables
SortKey
SortSpecification
SortSpecificationList
Statement
StatementWithoutSemicolon
TableContentsSource
TableDefinition
TableElement
TableElementList
TableElements
TableProperties
TableProperty
TablePropertyList
TablePropertyValue
Val
Vals
Where
WindowClause
WindowOrderClause

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
OptOrdering
OptFieldList
IdentifierList
RowValueList
RowValue
FieldValue
.

Terminals

and_
asc
asterisk
boolean
by
character_literal
comma
create
desc
describe
double
equals_operator
explain
false
first
float
from
greater_than_operator
gte
identifier
insert
integer
into
is_
group
key
last
limit
left_paren
less_than_operator
lte
minus_sign
nomatch
not_
null
nulls
offset
or_
order
plus_sign
primary
quantum
regex
right_paren
select
semicolon
show
sint64
solidus
table
tables
timestamp
true
values
varchar
where
with
.

Rootsymbol Statement.
Endsymbol '$end'.

Statement -> StatementWithoutSemicolon : '$1'.
Statement -> StatementWithoutSemicolon semicolon : '$1'.

GroupBy -> group by Fields: {group_by, '$3'}.

Query -> Select WindowClause : make_window_clause('$1', '$2').
Query -> Select              : make_window_clause('$1', make_orderby([], undefined, undefined)).

WindowClause -> WindowOrderClause: '$1'.
WindowOrderClause -> order by SortSpecificationList                                : make_orderby('$3', undefined, undefined).
WindowOrderClause -> order by SortSpecificationList LimitClause                    : make_orderby('$3', '$4', undefined).
WindowOrderClause -> order by SortSpecificationList LimitClause ResultOffsetClause : make_orderby('$3', '$4', '$5').
WindowOrderClause ->                                LimitClause                    : make_orderby([], '$1', undefined).
WindowOrderClause ->                                LimitClause ResultOffsetClause : make_orderby([], '$1', '$2').

LimitClause -> limit integer         : '$2'.
ResultOffsetClause -> offset integer : '$2'.

SortSpecificationList -> SortSpecification: ['$1'].
SortSpecificationList -> SortSpecification comma SortSpecificationList: ['$1' | '$3'].
SortSpecification -> SortKey                            : make_sort_spec('$1', undefined, undefined).
SortSpecification -> SortKey OrderingSpec               : make_sort_spec('$1', '$2', undefined).
SortSpecification -> SortKey              NullOrderSpec : make_sort_spec('$1', undefined, '$2').
SortSpecification -> SortKey OrderingSpec NullOrderSpec : make_sort_spec('$1', '$2', '$3').

SortKey -> Identifier: '$1'.
OrderingSpec -> asc : '$1'.
OrderingSpec -> desc: '$1'.
NullOrderSpec -> nulls first: {nulls_first, <<"nulls first">>}.  %% combine tokens
NullOrderSpec -> nulls last : {nulls_last, <<"nulls last">>}.

StatementWithoutSemicolon -> Query           : convert('$1').
StatementWithoutSemicolon -> TableDefinition : fix_up_keys('$1').
StatementWithoutSemicolon -> Describe : '$1'.
StatementWithoutSemicolon -> Explain : '$1'.
StatementWithoutSemicolon -> Insert : '$1'.
StatementWithoutSemicolon -> ShowTables : '$1'.

Select -> select Fields from Bucket Where GroupBy
                                          : make_select('$1', '$2', '$3', '$4', '$5', '$6').
Select -> select Fields from Bucket Where : make_select('$1', '$2', '$3', '$4', '$5').
Select -> select Fields from Bucket       : make_select('$1', '$2', '$3', '$4').

%% EXPLAIN STATEMENT
Explain -> explain Query : make_explain('$2').

%% 20.9 DESCRIBE STATEMENT
Describe -> describe Bucket : make_describe('$2').

Insert -> insert into Identifier OptFieldList values RowValueList : make_insert('$3', '$4', '$6').

Where -> where BooleanValueExpression : make_where('$1', '$2').

Fields -> Fields     comma   FieldElem   : concat_select('$1', '$3').
Fields -> FieldElem                      : '$1'.

FieldElem -> Field : '$1'.
FieldElem -> Val   : '$1'.

Field -> NumericValueExpression : '$1'.
%Field -> Identifier    : canonicalise_col('$1').
Field -> asterisk : make_wildcard('$1').

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

%% NullComp is termed NullPredicatePart2 in the SQL spec, however it is more
%% clearly a NullComparator, so termed NullComp here.
NullPredicate -> Vals NullComp : make_expr('$1', '$2').
%% Comp is short for CompOp as termed in the SQL spec.
ComparisonPredicate -> Vals Comp Vals : make_expr('$1', '$2', '$3').

Vals -> NumericValueExpression : '$1'.
Vals -> regex            : '$1'.
Vals -> Val              : '$1'.

Val -> varchar            : '$1'.
Val -> CharacterLiteral   : '$1'.
Val -> TruthValue         : '$1'.

NullComp -> IsNull             : '$1'.
NullComp -> IsNotNull             : '$1'.

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

ShowTables -> show tables : [{type, show_tables}].

NotNull -> not_ null : '$1'.

IsNull -> is_ null : {is_null, '$1'}.
IsNotNull -> is_ not_ null: {is_not_null, '$2'}.

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
    NullPredicate : '$1'.
BooleanPredicand ->
    ComparisonPredicate : '$1'.
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

%% definition for a primary key without brackets for the local key
%% CREATE TABLE mytab1 (a varchar not null, ts timestamp not null, primary key (quantum(ts,30,'d')) );
KeyDefinition ->
    PrimaryKey left_paren KeyFieldList right_paren : return_error_flat("No local key specified.").
%% definition for a primary key with the local key brackets but no local key or
%% comma after the partition key
%% CREATE TABLE rts1130_6 (a varchar not null, ts timestamp not null, primary key ((quantum(ts,30,'d'))) );
KeyDefinition ->
    PrimaryKey left_paren left_paren KeyFieldList right_paren right_paren : return_error_flat("No local key specified.").
KeyDefinition ->
    PrimaryKey left_paren left_paren KeyFieldList right_paren comma KeyFieldList right_paren : make_partition_and_local_keys('$4', '$7').

KeyFieldList -> KeyField comma KeyFieldList : ['$1' | '$3'].
KeyFieldList -> KeyField : ['$1'].

KeyField -> quantum left_paren KeyFieldArgList right_paren :
    element(2, make_modfun(quantum, '$3')).
KeyField -> Identifier OptOrdering  : 
    ?SQL_PARAM{name = [element(2, '$1')], ordering = '$2'}.

OptOrdering -> '$empty' : undefined.
OptOrdering -> asc : ascending.
OptOrdering -> desc : descending.

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

FieldValue -> null : '$1'.
FieldValue -> integer : '$1'.
FieldValue -> float : '$1'.
FieldValue -> TruthValue : '$1'.
FieldValue -> CharacterLiteral : '$1'.
FieldValue -> Identifier : '$1'.

TableProperties ->
    left_paren TablePropertyList right_paren : '$2'.

TablePropertyList ->
    '$empty' : [].
TablePropertyList ->
    TableProperty : prepend_table_proplist([], '$1').
TablePropertyList ->
    TableProperty comma TablePropertyList : prepend_table_proplist('$3', '$1').

TableProperty ->
    identifier equals_operator TablePropertyValue :
        make_table_property('$1', '$3').

TablePropertyValue -> identifier : '$1'.   %% this is not valid
%% the above rule is just to produce a specific error message
TablePropertyValue -> TruthValue : '$1'.
TablePropertyValue -> integer : '$1'.
TablePropertyValue -> float : '$1'.
TablePropertyValue -> character_literal : '$1'.

Erlang code.

-record(outputs,
        {
          type :: create | describe | explain | insert | select,
          buckets = [],
          fields  = [],
          where   = [],
          ops     = [],
          group_by,
          order_by = [],
          limit    = [],
          offset   = []
         }).

-include("riak_ql_ddl.hrl").

%% export the return value function to prevent xref errors
%% this fun is used during the parsing and is marked as
%% unused/but not to be exported in the yecc source
%% no way to stop rebar borking on it AFAIK
-export([
         return_error/2,
         ql_parse/1,
         canonicalise_where/1
         ]).

%% Provide more useful success tuples
ql_parse(Tokens) ->
    interpret_parse_result(parse(Tokens)).

interpret_parse_result({error, _}=Err) ->
    Err;
interpret_parse_result({ok, {?DDL{}=DDL, Props}}) ->
    {ddl, DDL, Props};
interpret_parse_result({ok, Proplist}) ->
    extract_type(proplists:get_value(type, Proplist), Proplist).

extract_type(Type, Proplist) ->
    {Type, Proplist -- [{type, Type}]}.

%% if no partition key is specified hash on the local key
fix_up_keys(?DDL{partition_key = none, local_key = LK} = DDL) ->
    DDL?DDL{partition_key = LK, local_key = LK};
fix_up_keys(A) ->
    A.

convert(#outputs{type     = select,
                 buckets  = B,
                 fields   = F,
                 where    = W,
                 group_by = G,
                 limit    = L,
                 offset   = Of,
                 order_by = Ob} = Outputs) ->
    ok = validate_select_query(Outputs),
    [
     {type, select},
     {tables,   B},
     {fields,   F},
     {where,    W},
     {group_by, G},
     {limit,    L},
     {offset,   Of},
     {order_by, Ob}
    ];
convert(#outputs{type = create} = O) ->
    O.

validate_select_query(Outputs) ->
    ok = assert_group_by_select(Outputs),
    ok = assert_group_by_is_valid(Outputs),
    ok = assert_group_by_with_order_by(Outputs).

%% If the query uses GROUP BY then check that the identifiers in the select
%% all exist in the GROUP BY.
assert_group_by_select(#outputs{ group_by = [] }) ->
    ok;
assert_group_by_select(#outputs{ fields = Fields, group_by = GroupBy }) ->
    Identifiers = lists:flatten(
        [lists:reverse(find_group_identifiers(ColumnSelect, [])) || ColumnSelect <- Fields]),
    IllegalIdentifiers =
        [to_identifier_name(Identifier)|| Identifier <- Identifiers, not is_identifier_in_groups(Identifier, GroupBy)],
    case IllegalIdentifiers of
        [] ->
            ok;
        _ ->
            return_error_flat("Field(s) " ++ string:join(IllegalIdentifiers,", ") ++ " are specified in the select statement but not the GROUP BY.")
    end.


%%
assert_group_by_is_valid(#outputs{ group_by = GroupBy }) ->
    case lists:member({identifier, [<<"*">>]}, GroupBy) of
        false ->
            ok;
        true ->
            return_error_flat("GROUP BY can only contain table columns but '*' was found.")
    end.

assert_group_by_with_order_by(#outputs{group_by = GroupBy, order_by = OrderBy})
  when (GroupBy /= [] andalso OrderBy /= []) ->
    return_error_flat("ORDER BY/LIMIT/OFFSET clauses are not supported for GROUP BY queries.");
assert_group_by_with_order_by(_) ->
    ok.


%%
is_identifier_in_groups({identifier, [F]}, GroupBy) ->
    lists:member({identifier, F}, GroupBy);
is_identifier_in_groups(Identifier, GroupBy) ->
    lists:member(Identifier, GroupBy).

%% Identifier field name as a string.
to_identifier_name({identifier, [F]}) ->
    binary_to_list(F);
to_identifier_name({identifier, F}) ->
    binary_to_list(F).

%% Recurse through a column in the select clause to find identifiers that must
%% be specified in the GROUP BY.
find_group_identifiers({identifier, [<<"*">>]} = Identifier, Acc) ->
    [Identifier|Acc];
find_group_identifiers({identifier, _} = Identifier, Acc) ->
    [Identifier|Acc];
find_group_identifiers({negate, Expr}, Acc) ->
    find_group_identifiers(Expr, Acc);
find_group_identifiers({Op, Left, Right}, Acc) when is_atom(Op) ->
    find_group_identifiers(Right, find_group_identifiers(Left, Acc));
find_group_identifiers({{window_agg_fn, _}, _}, Acc) ->
    %% identifiers in aggregate functions are ok
    Acc;
find_group_identifiers({_, _}, Acc) ->
    Acc.

make_select({select, multi_table_error}, _B, _C, _D) ->
    return_error(0, <<"Must provide exactly one table name">>);
make_select(A, B, C, D) ->
    make_select(A, B, C, D, {where, []}).

make_select(A, B, C, D, E) -> make_select(A, B, C, D, E, {group_by, []}).

make_select({select, _SelectBytes},
            Select,
            {from, _FromBytes},
            {Type, D},
            {_Where, E},
            {group_by, GroupFields}) ->
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
    #outputs{type    = select,
             fields  = FieldsWrappedIdentifiers,
             buckets = Bucket,
             where   = E,
             group_by = lists:flatten([GroupFields])
            }.


wrap_identifier({identifier, IdentifierName})
  when is_binary(IdentifierName) ->
    {identifier, [IdentifierName]};
wrap_identifier(Default) -> Default.

make_describe({identifier, D}) ->
    [
     {type, describe},
     {identifier, D}
    ].

%% For explain just change the output type
make_explain(#outputs{type = select} = S) ->
    Props = convert(S),
    lists:keyreplace(type, 1, Props, {type, explain}).

make_insert({identifier, Table}, Fields, Values) ->
    FieldsAsList = case is_list(Fields) of
                       true  -> Fields;
                       false -> []
                   end,
    FieldsWrappedIdentifiers = [wrap_identifier(X) || X <- FieldsAsList],
    [
     {type, insert},
     {table, Table},
     {fields, FieldsWrappedIdentifiers},
     {values, Values}
    ].


%% per Product Requirements
%% "If NULLS LAST is specified, null values sort after all non-null
%%  values; if NULLS FIRST is specified, null values sort before all
%%  non-null values. If neither is specified, the default behavior is
%%  NULLS LAST when ASC is specified or implied, and NULLS FIRST when
%%  DESC is specified (thus, the default is to act as though nulls are
%%  larger than non-nulls)."
make_sort_spec(Fld, {asc, _} = OrdSpec, undefined) ->
    {Fld, OrdSpec, {nulls_last, <<"nulls last">>}};
make_sort_spec(Fld, {desc, _} = OrdSpec, undefined) ->
    {Fld, OrdSpec, {nulls_first, <<"nulls first">>}};
make_sort_spec(Fld, OrdSpec, NullSpec) ->
    {Fld, make_ord_spec(OrdSpec), make_null_spec(NullSpec)}.

make_ord_spec(undefined) -> {asc, <<"asc">>};
make_ord_spec(OrdSpec) -> OrdSpec.
make_null_spec(undefined) -> {nulls_last, <<"nulls last">>};
make_null_spec(NullSpec) -> NullSpec.


make_orderby(OrdBy, Lim, Off) ->
    {order_by,
     [{F, OrdSpec, NullSpec} ||
         {{identifier, F}, {OrdSpec, _OrdSpecToken}, {NullSpec, _NullSpecToken}} <- OrdBy],
     make_limit(Lim), make_offset(Off)}.

make_limit({integer, A}) -> [A];
make_limit(undefined)    -> [].
make_offset({integer, A}) -> [A];
make_offset(undefined)    -> [].

make_window_clause(QueryExprBody, {order_by, OrderBy, Limit, Offset}) ->
    QueryExprBody#outputs{order_by = OrderBy,
                          limit    = Limit,
                          offset   = Offset}.


make_expr({TypeA, A}, {B, _}) ->
    {expr, {B, {TypeA, A}}}.
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

canon2({ComparisonPredicate, A, B}) when is_binary(B) andalso not is_binary(A) ->
    canonicalize_condition_order({ComparisonPredicate, B, A});
canon2({ComparisonPredicate, A, B}) when ComparisonPredicate =:= and_ orelse
                                         ComparisonPredicate =:= or_  ->
    %% this is stack busting non-tail recursion
    %% but our where clauses are bounded in size so thats OK
    A1 = canon2(A),
    B1 = canon2(B),
    case A1 == B1 of
        true ->
            A1;
        false ->
            case is_lower(A1, B1) of
                true  -> {ComparisonPredicate, A1, B1};
                false -> {ComparisonPredicate, B1, A1}
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

hoist({A, B}) ->
    {A, B};
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

is_lower({NullOp,{identifier, _}}, {_, _, _}) when NullOp =:= is_null orelse
                                                   NullOp =:= is_not_null ->
    false;
is_lower({_, _, _}, {NullOp,{identifier, _}}) when NullOp =:= is_null orelse
                                                   NullOp =:= is_not_null ->
    false;
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

make_partition_and_local_keys(PFields, LFields) ->
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

make_table_definition(TableName, Contents) ->
    make_table_definition(TableName, Contents, []).

make_table_definition({identifier, Table}, Contents, Properties) ->
    DDL1 = ?DDL{
        table = Table,
        partition_key = find_partition_key(Contents),
        local_key = find_local_key(Contents),
        fields = find_fields(Contents) },
    %% validate here so code afterwards doesn't require error checks
    ok = validate_ddl(DDL1),
    DDL2 = DDL1?DDL{
        minimum_capability = riak_ql_ddl:get_minimum_capability(DDL1) },
    {DDL2, validate_table_properties(Properties)}.

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
                args = [?SQL_PARAM{name = [Param]}, Quantity, binary_to_existing_atom(Unit, utf8)],
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

make_table_property({identifier, K}, {Type, _V})
  when Type == identifier ->
    return_error(
      0, iolist_to_binary(
           io_lib:format("Expecting a numeric, boolean or string value for WITH property \"~s\""
                         " (did you forget to quote a string?)", [K])));
make_table_property({identifier, K}, {Type, V})
  when Type == boolean;
       Type == integer;
       Type == float;
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
    ok = assert_partition_key_fields_not_descending(DDL),
    ok = assert_not_more_than_one_quantum(DDL),
    ok = assert_quantum_fn_args(DDL),
    ok = assert_quantum_is_last_in_partition_key(DDL),
    ok = assert_desc_key_types(DDL),
    ok.

%% @doc Ensure DDL has keys
assert_keys_present(?DDL{local_key = LK, partition_key = PK})
  when LK == none;
       PK == none ->
    return_error_flat("Missing primary key");
assert_keys_present(_GoodDDL) ->
    ok.

%% @doc Ensure all fields appearing in PRIMARY KEY are not null.
assert_primary_key_fields_non_null(?DDL{local_key = #key_v1{ast = LK},
                                        fields = Fields}) ->
    PKFieldNames = [N || ?SQL_PARAM{name = [N]} <- LK],
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

%% @doc Assert that the partition key has at least one field.
assert_partition_key_length(?DDL{partition_key = {key_v1, [_|_]}}) ->
    ok;
assert_partition_key_length(?DDL{partition_key = {key_v1, Key}}) ->
    return_error_flat("Primary key must have one or more fields ~p", [Key]).

%% @doc Verify primary key and local partition have the same elements
assert_primary_and_local_keys_match(?DDL{partition_key = #key_v1{ast = Primary},
                                         local_key = #key_v1{ast = Local}}) ->
    PrimaryList = [query_field_name(F) || F <- Primary],
    LocalList = [query_field_name(F) || F <- lists:sublist(Local, length(PrimaryList))],
    case PrimaryList == LocalList of
        true ->
            ok;
        false ->
            return_error_flat("Local key does not match primary key")
    end.

%%
assert_partition_key_fields_not_descending(?DDL{ partition_key = #key_v1{ ast = PK }}) ->
    [ordering_in_partition_key_error(N, O)
        || ?SQL_PARAM{ name = [N], ordering = O } <- PK, O /= undefined],
    ok.

%%
-spec ordering_in_partition_key_error(binary(), atom()) -> no_return().
ordering_in_partition_key_error(N, Ordering) when is_binary(N) ->
    return_error_flat("Order can only be used in the local key, '~s' set to ~p", [N, Ordering]).

%%
assert_unique_fields_in_pk(?DDL{local_key = #key_v1{ast = LK}}) ->
    Fields = [N || ?SQL_PARAM{name = [N]} <- LK],
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
assert_partition_key_fields_exist(?DDL{ fields = Fields,
                                         partition_key = #key_v1{ ast = PK } }) ->
    MissingFields =
        [binary_to_list(name_of(F)) || F <- PK, not is_field(F, Fields)],
    case MissingFields of
        [] ->
            ok;
        _ ->
            return_error_flat("Primary key includes non-existent fields (~s).",
                              [string:join(MissingFields, ", ")])
    end.

assert_quantum_fn_args(?DDL{ partition_key = #key_v1{ ast = PKAST } } = DDL) ->
    [assert_quantum_fn_args2(DDL, Args) || #hash_fn_v1{ mod = riak_ql_quanta, fn = quantum, args = Args } <- PKAST],
    ok.

%% The param argument is validated by assert_partition_key_fields_exist/1.
assert_quantum_fn_args2(DDL, [Param, Unit, Measure]) ->
    FieldName = name_of(Param),
    case riak_ql_ddl:get_field_type(DDL, FieldName) of
        {ok, timestamp} ->
            ok;
        {ok, InvalidType} ->
            return_error_flat("Quantum field '~s' must be type of timestamp but was ~p.",
                              [FieldName, InvalidType])
    end,
    case lists:member(Measure, [d,h,m,s]) of
        true ->
            ok;
        false ->
            return_error_flat("Quantum time measure was ~p but must be d, h, m or s.",
                              [Measure])
    end,
    case is_integer(Unit) andalso Unit >= 1 of
        true ->
            ok;
        false ->
            return_error_flat("Quantum time unit must be a positive integer.", [])
    end.

assert_not_more_than_one_quantum(?DDL{ partition_key = #key_v1{ ast = PKAST } }) ->
    QuantumFns =
        [Fn || #hash_fn_v1{ } = Fn <- PKAST],
    case length(QuantumFns) =< 1 of
        true ->
            ok;
        false ->
            return_error_flat(
                "More than one quantum function in the partition key.", [])
    end.

assert_quantum_is_last_in_partition_key(?DDL{ partition_key = #key_v1{ ast = PKAST } }) ->
    assert_quantum_is_last_in_partition_key2(PKAST).

%%
assert_quantum_is_last_in_partition_key2([]) ->
    ok;
assert_quantum_is_last_in_partition_key2([#hash_fn_v1{ }]) ->
    ok;
assert_quantum_is_last_in_partition_key2([#hash_fn_v1{ }|_]) ->
    return_error_flat(
        "The quantum function must be the last element of the partition key.", []);
assert_quantum_is_last_in_partition_key2([_|Tail]) ->
    assert_quantum_is_last_in_partition_key2(Tail).

%% Assert that any paramerts in the local key that use the desc keyword are
%% types that support it.
assert_desc_key_types(?DDL{ local_key = #key_v1{ ast = LKAST } } = DDL) ->
    [assert_desc_key_field_type(DDL, P) || ?SQL_PARAM{ ordering = descending } = P <- LKAST],
    ok.

%%
assert_desc_key_field_type(DDL, ?SQL_PARAM{ name = [Name] }) ->
    {ok, Type} = riak_ql_ddl:get_field_type(DDL, Name),
    case Type of
        sint64 ->
            ok;
        varchar ->
            ok;
        timestamp ->
            ok;
        _ ->
            return_error_flat(
                "Elements in the local key marked descending (DESC) must be of "
                "type sint64 or varchar, but was ~p.", [Type])
    end.

%% Check that the field name exists in the list of fields.
is_field(Field, Fields) ->
    (lists:keyfind(name_of(Field), 2, Fields) /= false).

%%
name_of(?SQL_PARAM{ name = [N] }) ->
    N;
name_of(#hash_fn_v1{ args = [?SQL_PARAM{ name = [N] }|_] }) ->
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
    Param = lists:keyfind(?SQL_PARAM_RECORD_NAME, 1, Args),
    query_field_name(Param);
query_field_name(?SQL_PARAM{name = Field}) ->
    Field.

-spec return_error_flat(string()) -> no_return().
return_error_flat(F) ->
    return_error_flat(F, []).
-spec return_error_flat(string(), [term()]) -> no_return().
return_error_flat(F, A) ->
    return_error(
      0, iolist_to_binary(io_lib:format(F, A))).
