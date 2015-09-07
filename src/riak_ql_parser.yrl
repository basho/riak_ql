%% -*- erlang -*-
%%% @doc       Parser for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2015 Basho

Nonterminals

Statement
Query
Select
Insert
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
Val2
Vals
Vals2
Funcall
TableDefinition
TableContentsSource
TableElementList
TableElements
TableElement
ColumnDefinition
ColumnConstraint
ForeignKeyDefinition
KeyDefinition
DataType
KeyFieldList
KeyField
KeyFieldArgList
KeyFieldArg
.

Terminals

insert_into
select
from
limit
where
and_
 or_
on
%% delete
%% drop
%% groupby
%% merge
%% inner
inner_join
%% join
%% as
datetime
regex
quoted
int
int_type
float
float_type
foreign_key
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
primary_key
timestamp
varchar
atom
quantum
values
.

Rootsymbol Statement.
Endsymbol '$end'.

Statement -> Query : convert('$1').
Statement -> TableDefinition : fix_up_keys('$1').
Statement -> Insert : '$1'.

Insert -> insert_into Bucket openb Fields closeb values openb Vals2 closeb : make_insert('$2', '$4', '$8').

Query -> Select inner_join Bucket on Field eq Field : add_inner('$1', '$3', '$5', '$7').
Query -> Select limit int : add_limit('$1', '$2', '$3').
Query -> Select           : '$1'.

Select -> select Fields from Buckets Where : make_clause('$1', '$2', '$3', '$4', '$5').
Select -> select Fields from Buckets       : make_clause('$1', '$2', '$3', '$4').
Where -> where Conds : make_where('$1', '$2').

Fields -> Fields comma Field : make_list('$1', '$3').
Fields -> Field              : make_list('$1').

Field -> Word                : '$1'.
Field -> maybetimes          : '$1'.

Buckets -> Buckets comma Bucket : make_list('$1', '$3').
Buckets -> Bucket               : '$1'.

Bucket -> Word   : '$1'.
Bucket -> regex  : '$1'.
Bucket -> quoted : '$1'.

Word -> Word chars : concatenate('$1', '$2').
Word -> chars      : process('$1').

Funcall -> Word openb     closeb : make_funcall('$1').
Funcall -> Word openb Val closeb : make_funcall('$1').

Conds -> openb Conds closeb             : make_expr('$2').
Conds -> Conds Logic Cond               : make_expr('$1', '$2', '$3').
Conds -> Conds Logic openb Conds closeb : make_expr('$1', '$2', '$4').
Conds -> Cond                           : '$1'.

Cond -> Vals Comp Vals : make_expr('$1', '$2', '$3').

Vals2 -> Vals2 comma Val2 : make_list('$1', '$3').
Vals2 -> Val2 : make_list('$1').

Val2 -> Val  : '$1'.
Val2 -> Word : '$1'.

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
Val -> quoted    : make_word('$1').

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
TableElement -> ForeignKeyDefinition : '$1'.

ColumnDefinition ->
    Field DataType ColumnConstraint : make_column('$1', '$2', '$3').
ColumnDefinition ->
    Field DataType : make_column('$1', '$2').
ColumnConstraint -> not_null : '$1'.

DataType -> datetime : '$1'.
DataType -> float_type : canonicalize_data_type('$1').
DataType -> int_type : canonicalize_data_type('$1').
DataType -> timestamp : '$1'.
DataType -> varchar : '$1'.

ForeignKeyDefinition ->
    foreign_key openb Bucket comma KeyField closeb : make_foreign_key('$3', '$5').
    
KeyDefinition ->
    primary_key       openb KeyFieldList closeb                           : make_local_key('$3').
KeyDefinition ->
    primary_key openb openb KeyFieldList closeb comma KeyFieldList closeb : make_partition_and_local_keys('$4', '$7').

KeyFieldList -> KeyField comma KeyFieldList : make_list('$3', '$1').
KeyFieldList -> KeyField : make_list({list, []}, '$1').

KeyField -> quantum openb KeyFieldArgList closeb : make_modfun(quantum, '$3').
KeyField -> Word : '$1'.

KeyFieldArgList ->
    KeyFieldArg comma KeyFieldArgList : make_list('$3', '$1').
KeyFieldArgList ->
    KeyFieldArg : make_list({list, []}, '$1').

KeyFieldArg -> int    : '$1'.
KeyFieldArg -> float  : '$1'.
KeyFieldArg -> Word   : '$1'.
KeyFieldArg -> atom openb Word closeb : make_atom('$3').

Erlang code.

-record(outputs,
        {
          type       = [] :: select | create,
          buckets    = [],
          fields     = [],
	  inner_join = [],
	  on         = [],
          limit      = [],
          where      = [],
          ops        = []
         }).

-include("riak_ql_sql.hrl").
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

make_insert({word, Bucket}, {list, Fields}, {list, Vals}) ->
    Len1 = length(Fields),
    Len2 = length(Vals),
    case Len1 of
	Len2 -> #riak_sql_insert_v1{'INSERT INTO' = list_to_binary(Bucket),
				    'VALUES'      = lists:zip(Fields, Vals)}
		    ;
	_ -> exit("field list doesn't match values list")
    end.

%% if no partition key is specified hash on the local key
fix_up_keys(#ddl_v1{partition_key = none, local_key = LK} = DDL) ->
    DDL#ddl_v1{partition_key = LK, local_key = LK};
fix_up_keys(A) ->
    A.

convert(#outputs{type       = select,
		 buckets    = B,
		 fields     = F,
		 inner_join = II,
		 on         = O,
		 limit      = L,
		 where      = W}) ->
    Q = case B of
	    {Type, _} when Type =:= list orelse Type =:= regex ->
		#riak_sql_v1{'SELECT'     = F,
			     'FROM'       = B,
			     'WHERE'      = W,
			     'INNER JOIN' = II,
			     'ON'         = O,
			     'LIMIT'      = L};
	    _ ->
		#riak_sql_v1{'SELECT'     = F,
			     'FROM'       = B,
			     'WHERE'      = W,
			     'INNER JOIN' = II,
			     'ON'         = O,
			     'LIMIT'      = L,
			     helper_mod = riak_ql_ddl:make_module_name(B)}
	end,
    Q;
convert(#outputs{type = create} = O) ->
    O;
convert(X) ->
    io:format("X is ~p~n", [X]),
    X.

process({chars, A}) ->
    {word, A}.

concatenate({word, A}, {chars, B}) ->
    {word, A ++ B}.

make_atom({word, SomeWord}) ->
    {atom, list_to_atom(SomeWord)}.

make_clause(A, B, C, D) -> make_clause(A, B, C, D, {where, []}).

make_clause({select, _A}, {_, B}, {from, _C}, {Type, D}, {_, E}) ->
    Type2 = case Type of
                list   -> list;
                word   -> string;
                quoted -> string;
                regex  -> regex
            end,
    Bucket = case Type2 of
		 string -> list_to_binary(D);
		 list   -> {Type2, [list_to_binary(X) || X <- D]};
		 regex  -> {Type2, D}
	     end,
    _O = #outputs{type    = select,
                  fields  = [[X] || X <- B],
                  buckets = Bucket,
                  where   = E
                 }.

add_inner(A, {word, B}, {word, C}, {word, D}) ->
    A#outputs{inner_join = list_to_binary(B),
	      on         = {C, D}}.

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
    {conditional, {B1, A, C2}}.

make_word({quoted, Q}) -> {word, Q}.

make_where({where, A}, {conditional, B}) ->
    NewB = remove_conditionals(B),
    {A, [canonicalise(NewB)]}.

%%
%% rewrite the where clause to have a canonical form
%% makes query rewriting easier
%%
canonicalise(WhereClause) ->
    Canonical = canon2(WhereClause),
    _NewWhere = hoist(Canonical).

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

make_funcall({A, B}) ->
     make_funcall({A, B}, []).

make_funcall({_A, B}, C) ->
    {funcall, {B, C}}.

add_unit({Type, A}, {chars, U}) when U =:= "s" -> {Type, A};
add_unit({Type, A}, {chars, U}) when U =:= "m" -> {Type, A*60};
add_unit({Type, A}, {chars, U}) when U =:= "h" -> {Type, A*60*60};
add_unit({Type, A}, {chars, U}) when U =:= "d" -> {Type, A*60*60*24}.

make_list({maybetimes, A}) -> {list, [A]};
make_list({word,       A}) -> {list, [A]};
make_list(A)               -> {list, [A]}.

make_list({list, A}, {_, B}) -> {list, A ++ [B]};
make_list({_,    A}, {_, B}) -> {list, [A, B]}.

make_expr(A) ->
    {conditional, A}.

make_column({word, FieldName}, {DataType, _}) ->
    #riak_field_v1{
       name = FieldName,
       type = canonicalize_field_type(DataType),
       optional = true}.

make_column({word, FieldName}, {DataType, _}, {not_null, _}) ->
    #riak_field_v1{
       name = FieldName,
       type = canonicalize_field_type(DataType),
       optional = false}.

make_foreign_key({word, Bucket}, {word, Field}) ->
    {foreign_key, list_to_binary(Bucket), Field}.

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
    {table_element_list, [A, B]}.

extract_key_field_list({list, []}, Extracted) ->
    Extracted;
extract_key_field_list({list,
                        [Modfun = #hash_fn_v1{} | Rest]},
                       Extracted) ->
    [Modfun | extract_key_field_list({list, Rest}, Extracted)];
extract_key_field_list({list, [Field | Rest]}, Extracted) ->
    [#param_v1{name = [Field]} |
     extract_key_field_list({list, Rest}, Extracted)].

make_table_definition({word, BucketName}, {table_element_list, Contents}) ->
    PartitionKey = find_partition_key(Contents),
    LocalKey = find_local_key(Contents),
    {foreign_key, Bucket, FKField} = find_foreign_key(Contents),
    Fields = find_fields(Contents),
    #ddl_v1{
       bucket         = list_to_binary(BucketName),
       partition_key  = PartitionKey,
       local_key      = LocalKey,
       fields         = Fields,
       foreign_bucket = Bucket,
       foreign_key    = FKField}.

find_foreign_key([]) ->
    {foreign_key, none, none};
find_foreign_key([{foreign_key, _, _} = FK | _Rest]) ->
    FK;
find_foreign_key([_Head | Rest]) ->
    find_foreign_key(Rest).

find_partition_key([]) ->
    none;
find_partition_key([{partition_key, Key} | _Rest]) ->
    Key;
find_partition_key([_Head | Rest]) ->
    find_partition_key(Rest).

find_local_key([]) ->
    none;
find_local_key([{local_key, Key} | _Rest]) ->
    Key;
find_local_key([_Head | Rest]) ->
    find_local_key(Rest).

make_modfun(quantum, {list, Args}) ->
    [Param, Quantity, Unit] = lists:reverse(Args),
    {modfun, #hash_fn_v1{
		mod  = riak_ql_quanta,
		fn   = quantum,
		args = [#param_v1{name = [Param]}, Quantity, list_to_existing_atom(Unit)],
		type = timestamp
	       }}.

find_fields(Elements) ->
    find_fields(1, Elements, []).

find_fields(_Count, [], Found) ->
    lists:reverse(Found);
find_fields(Count, [Field = #riak_field_v1{} | Rest], Elements) ->
    PositionedField = Field#riak_field_v1{position = Count},
    find_fields(Count + 1, Rest, [PositionedField | Elements]);
find_fields(Count, [_Head | Rest], Elements) ->
    find_fields(Count, Rest, Elements).

canonicalize_field_type(varchar) ->
    binary;
canonicalize_field_type(Type) ->
    Type.

canonicalize_data_type({float_type, Tokens}) ->
    {float, Tokens};
canonicalize_data_type({int_type, Tokens}) ->
    {integer, Tokens};
canonicalize_data_type(Type) ->
    Type.
