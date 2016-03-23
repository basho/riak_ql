# Riak QL

riak_ql provides a number of different functions to Riak
* a lexer/parser for the SQL sub-set we support
* a function for calculating time quanta for Time Series
* a compiler for generating helper modules to validate and manipulate records that correspond to a defined table schema in the DDL

Link to the official and still private [docs](https://github.com/basho/private_basho_docs/tree/timeseries/1.0.0/source/languages/en/riakts).

### Compiling and Decompiling

```
Table_def = "CREATE TABLE MyTable (myfamily varchar not null, myseries varchar not null, time timestamp not null, weather varchar not null, temperature double, PRIMARY KEY ((myfamily, myseries, quantum(time, 10, 'm')), myfamily, myseries, time))".

{ok, DDL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def)).
{ModName, AST} = riak_ql_ddl_compiler:compile(DDL).

riak_ql_ddl_compiler:write_source_to_files("/tmp", DDL, AST).
```

### Generated Modules

The structure and interfaces of the generated modules is shown as per this `.erl` file which has been reverse generated from the AST that riak_kv_ddl_compiler emits. The comments contain details of the fields and keys used in the creation of the DDL.

```erlang
%%% Generated Module, DO NOT EDIT

%%% Validates the DDL

%%% Table         : temperatures
%%% Fields        : [{riak_field_v1,<<"a">>,1,varchar,false},
                     {riak_field_v1,<<"b">>,2,varchar,false},
                     {riak_field_v1,<<"c">>,3,timestamp,false}]
%%% Partition_Key : {key_v1,
                        [{param_v1,[<<"a">>],undefined},
                         {param_v1,[<<"b">>],undefined},
                         {hash_fn_v1,riak_ql_quanta,quantum,
                             [{param_v1,[<<"c">>],undefined},15,s],
                             timestamp}]}
%%% Local_Key     : {key_v1,[{param_v1,[<<"a">>],ascending},
                             {param_v1,[<<"b">>],descending},
                             {param_v1,[<<"c">>],ascending}]}


-module('riak_ql_table_temperatures$1').

-export([validate_obj/1, add_column_info/1,
     get_field_type/1, get_field_position/1,
     get_field_positions/0, is_field_valid/1, extract/2,
     get_ddl/0, field_orders/0]).

validate_obj({Var1_a, Var2_b, Var3_c})
    when is_integer(Var3_c) andalso Var3_c > 0,
     is_binary(Var2_b), is_binary(Var1_a) ->
    true;
validate_obj(_) -> false.

add_column_info({Var1_a, Var2_b, Var3_c}) ->
    [{<<"a">>, Var1_a}, {<<"b">>, Var2_b},
     {<<"c">>, Var3_c}].

extract(Obj, [<<"a">>]) when is_tuple(Obj) ->
    element(1, Obj);
extract(Obj, [<<"b">>]) when is_tuple(Obj) ->
    element(2, Obj);
extract(Obj, [<<"c">>]) when is_tuple(Obj) ->
    element(3, Obj).

get_field_type([<<"a">>]) -> varchar;
get_field_type([<<"b">>]) -> varchar;
get_field_type([<<"c">>]) -> timestamp.

get_field_position([<<"a">>]) -> 1;
get_field_position([<<"b">>]) -> 2;
get_field_position([<<"c">>]) -> 3;
get_field_position(_Other) -> undefined.

get_field_positions() ->
    [{[<<"a">>], 1}, {[<<"b">>], 2}, {[<<"c">>], 3}].

is_field_valid([<<"a">>]) -> true;
is_field_valid([<<"b">>]) -> true;
is_field_valid([<<"c">>]) -> true;
is_field_valid([<<"*">>]) -> true;
is_field_valid(_) -> false.

get_ddl() ->
    {ddl_v1, <<"temperatures">>,
     [{riak_field_v1, <<"a">>, 1, varchar, false},
      {riak_field_v1, <<"b">>, 2, varchar, false},
      {riak_field_v1, <<"c">>, 3, timestamp, false}],
     {key_v1,
      [{param_v1, [<<97>>], undefined},
       {param_v1, [<<98>>], undefined},
       {hash_fn_v1, riak_ql_quanta, quantum,
    [{param_v1, [<<99>>], undefined}, 15, s], timestamp}]},
     {key_v1,
      [{param_v1, [<<97>>], ascending},
       {param_v1, [<<98>>], descending},
       {param_v1, [<<99>>], ascending}]}}.

field_orders() -> [ascending, descending, ascending].
```

### Keywords in the Lexer

The keyword definitions in the lexer are formulaic and repetitive. They're generated using the `priv/riak_ql_keywords.csv` file and `priv/keyword_generator.rb` script.

To add new keywords to the lexer:

1. Add them to the `priv/riak_ql_keywords.csv` file, one per line. Keeping this file in alphabetic order simplifies future changes.
2. Run `ruby priv/keyword_generator.rb` with Ruby 1.9.3 or newer.
3. Replace the keyword definitions near the top of `src/riak_ql_lexer.xrl` with the regex definitions (first chunk of output) from the message generator.
4. Replace the keyword rules in the `src/riak_ql_lexer.xrl` file with the second chunk of output from the message generator.
5. Save and commit the csv and lexer changes.

If this is done correctly, the commit diff should simply be a few lines added to the csv and a few lines added to the lexer.
