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
Word
Where
Cond
Conds
Comp
Logic
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
.

Terminals

select
from
limit
where
and_
 or_
%% delete
%% drop
%% groupby
%% merge
%% inner
%% join
%% as
datetime
regex
quoted
int
float
eq
gt
lt
ne
nomatch
approx
notapprox
openb
closeb
plus
minus
maybetimes
div_
comma
chars
create_table
not_null
partition_key
timestamp
varchar
local_key
.

Rootsymbol Statement.
Endsymbol '$end'.

Statement -> Query : '$1'.
Statement -> TableDefinition : '$1'.

Query -> Select limit int : add_limit('$1', '$2', '$3').
Query -> Select           : '$1'.

Select -> select Fields from Buckets Where : make_clause('$1', '$2', '$3', '$4', '$5').
Select -> select Fields from Buckets       : make_clause('$1', '$2', '$3', '$4').
Where -> where Conds : make_where('$1', '$2').

Fields -> Fields comma Field : make_list('$1', '$3').
Fields -> Field              : '$1'.

Field -> Word                : log('$1', "make field").
Field -> maybetimes          : log('$1', "make field").

Buckets -> Buckets comma Bucket : make_list('$1', '$3').
Buckets -> Bucket               : '$1'.

Bucket -> Word   : '$1'.
Bucket -> regex  : log('$1', "buckets -> regex").
Bucket -> quoted : log('$1', "buckets -> quoted").

Word -> Word chars : concatenate('$1', '$2').
Word -> chars      : process('$1').

Funcall -> Word openb     closeb : make_funcall('$1').
Funcall -> Word openb Val closeb : make_funcall('$1').

Conds -> openb Conds closeb  : make_expr('$2').
Conds -> Conds Logic Cond    : make_expr('$1', '$2', '$3').
Conds -> Cond                : '$1'.

Cond -> Vals Comp Vals : make_expr('$1', '$2', '$3').

Vals -> Vals plus       Val : make_expr('$1', '$2', '$3').
Vals -> Vals minus      Val : make_expr('$1', '$2', '$3').
Vals -> Vals maybetimes Val : make_expr('$1', '$2', '$3').
Vals -> Vals div_       Val : make_expr('$1', '$2', '$3').
Vals -> regex               : '$1'.
Vals -> Val                 : '$1'.
Vals -> Funcall             : '$1'.
Vals -> Word                : '$1'.

Val -> int chars : add_unit('$1', '$2').
Val -> int       : '$1'.
Val -> float     : '$1'.
Val -> datetime  : '$1'.
Val -> varchar   : '$1'.

Logic -> and_ : '$1'.
Logic -> or_  : '$1'.

Comp -> approx    : '$1'.
Comp -> eq        : '$1'.
Comp -> gt        : '$1'.
Comp -> lt        : '$1'.
Comp -> ne        : '$1'.
Comp -> nomatch   : '$1'.
Comp -> notapprox : '$1'.

%% TABLE DEFINTITION

TableDefinition ->
    create_table Bucket TableContentsSource :
        make_table_definition('$2', '$3').

TableContentsSource -> TableElementList : '$1'.
TableElementList -> openb TableElements closeb : '$2'.

TableElements ->
    TableElement comma TableElements : make_table_element_list('$1', '$3').
TableElements -> TableElement : '$1'.

TableElement -> ColumnDefinition : '$1'.
TableElement -> KeyDefinition : '$1'.

ColumnDefinition ->
    Field DataType ColumnConstraint : make_column('$1', '$2', '$3').
ColumnDefinition ->
    Field DataType : make_column('$1', '$2').
ColumnConstraint -> not_null : '$1'.

DataType -> datetime : '$1'.
DataType -> float : '$1'.
DataType -> int : '$1'.
DataType -> timestamp : '$1'.
DataType -> varchar : '$1'.

KeyDefinition ->
    partition_key openb Field closeb : make_key_definition('$1', '$3').
KeyDefinition ->
    local_key openb Field closeb : make_key_definition('$1', '$3').

Erlang code.

-compile(export_all).

-record(outputs,
        {
          type    = [] :: select | drop | delete,
          fields  = [],
          buckets = [],
          limit   = none,
          where   = none,
          ops     = []
         }).

-include("riak_ql.yrl.tests").
-include("riak_kv_ddl.hrl").

process({chars, A}) ->
    {word, A}.

concatenate({word, A}, {chars, B}) ->
    {word, A ++ B}.

make_clause(A, B, C, D) -> make_clause(A, B, C, D, {where, none}).

make_clause({select, A}, {_, B}, {from, _C}, {Type, D}, {_, E}) ->
    Type2 = case Type of
                list   -> list;
                word   -> string;
                quoted -> string;
                regex  -> regex
            end,
    _O = #outputs{type    = list_to_existing_atom(A),
                  fields  = B,
                  buckets = {Type2, D},
                  where   = E
                 }.

log(A, _Str) ->
    %% Msg = io_lib:format(Str ++ " ~p~n", [A]),
    %% bits:log_terms(lists:flatten(Msg)),
    A.

add_limit(A, _B, {int, C}) ->
    A#outputs{limit = C}.

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
    {conditional, {B1, {A, C2}}}.

make_where({where, A}, {conditional, B}) ->
    {A, B}.

make_funcall({A, B}) ->
     make_funcall({A, B}, []).

make_funcall({_A, B}, C) ->
    %% Msg = io_lib:format("in make_funcall with ~p and ~p~n", [A, B]),
    %% bits:log_terms(lists:flatten(Msg)),
    {funcall, {B, C}}.

add_unit({Type, A}, {chars, U}) when U =:= "s" -> {Type, A};
add_unit({Type, A}, {chars, U}) when U =:= "m" -> {Type, A*60};
add_unit({Type, A}, {chars, U}) when U =:= "h" -> {Type, A*60*60};
add_unit({Type, A}, {chars, U}) when U =:= "d" -> {Type, A*60*60*24}.

make_list({list, A}, {_, B}) -> {list, A ++ [B]};
make_list({_,    A}, {_, B}) -> {list, [A, B]}.

make_expr(A) ->
    {conditional, A}.

make_column({word, FieldName}, {DataType, _}) ->
    #riak_field_v1{
       name = FieldName,
       type = DataType,
       optional = true}.

make_column({word, FieldName}, {DataType, _}, {not_null, _}) ->
    #riak_field_v1{
       name = FieldName,
       type = DataType,
       optional = false}.

make_key_definition({partition_key, _}, {word, FieldName}) ->
    #partition_key_v1{
       ast = [#param_v1{
                 name = FieldName
                }]};
make_key_definition({local_key, _}, {word, FieldName}) ->
    #local_key_v1{
       ast = [#param_v1{
                 name = FieldName
                }]}.

make_table_element_list(A, {table_element_list, B}) ->
    {table_element_list, [A] ++ B};
make_table_element_list(A, B) ->
    {table_element_list, [A, B]}.


make_table_definition({word, BucketName}, Contents) ->
    PartitionKey = find_partition_key(Contents),
    LocalKey = find_local_key(Contents),
    Fields = find_fields(Contents),
    #ddl_v1{
       bucket = list_to_binary(BucketName),
       partition_key = PartitionKey,
       local_key = LocalKey,
       fields = Fields}.

find_partition_key({table_element_list, Elements}) ->
    find_partition_key(Elements);
find_partition_key([]) ->
    undefined;
find_partition_key([PartitionKey = #partition_key_v1{} | _Rest]) ->
    PartitionKey;
find_partition_key([_Head | Rest]) ->
    find_partition_key(Rest).

find_local_key({table_element_list, Elements}) ->
    find_local_key(Elements);
find_local_key([]) ->
    undefined;
find_local_key([LocalKey = #local_key_v1{} | _Rest]) ->
    LocalKey;
find_local_key([_Head | Rest]) ->
    find_local_key(Rest).



find_fields({table_element_list, Elements}) ->
    find_fields(0, Elements, []).

find_fields(_Count, [], Found) ->
    lists:reverse(Found);
find_fields(Count, [Field = #riak_field_v1{} | Rest], Elements) ->
    PositionedField = Field#riak_field_v1{position = Count},
    find_fields(Count + 1, Rest, [PositionedField | Elements]);
find_fields(Count, [_Head | Rest], Elements) ->
    find_fields(Count, Rest, Elements).
