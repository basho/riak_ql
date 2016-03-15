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
    %%% 
    %%% Validates the DDL
    %%% 
    %%% Table         : timeseries_filter_test
    %%% Fields        : [{riak_field_v1,<<"geohash">>,1,varchar,false},
    %%%                  {riak_field_v1,<<"user">>,2,varchar,false},
    %%%                  {riak_field_v1,<<"time">>,3,timestamp,false},
    %%%                  {riak_field_v1,<<"weather">>,4,varchar,false},
    %%%                  {riak_field_v1,<<"temperature">>,5,varchar,true}]
    %%% Partition_Key : {key_v1,[{hash_fn_v1,riak_ql_quanta,quantum,
    %%%                                      [{param_v1,[<<"time">>]},15,s],
    %%%                                      undefined}]}
    %%% Local_Key     : {key_v1,[{param_v1,[<<"time">>]},{param_v1,[<<"user">>]}]}
    %%% 
    %%% 
    -module('riak_ql_table_timeseries_filter_test$1').
    
    -export([validate_obj/1, add_column_info/1,
    	 get_field_type/1, is_field_valid/1, extract/2,
         get_field_position/1, get_field_positions/0,
    	 get_ddl/0]).
    
    validate_obj({Var1_geohash, Var2_user, Var3_time,
    	      Var4_weather, Var5_temperature})
        when Var5_temperature =:= [] orelse
    	   is_binary(Var5_temperature),
    	 is_binary(Var4_weather),
    	 is_integer(Var3_time) andalso Var3_time > 0,
    	 is_binary(Var2_user), is_binary(Var1_geohash) ->
        true;
    validate_obj(_) -> false.
    
    add_column_info({Var1_geohash, Var2_user, Var3_time,
    		 Var4_weather, Var5_temperature}) ->
        [{<<"geohash">>, Var1_geohash}, {<<"user">>, Var2_user},
         {<<"time">>, Var3_time}, {<<"weather">>, Var4_weather},
         {<<"temperature">>, Var5_temperature}].
    
    extract(Obj, [<<"geohash">>]) when is_tuple(Obj) ->
        element(1, Obj);
    extract(Obj, [<<"user">>]) when is_tuple(Obj) ->
        element(2, Obj);
    extract(Obj, [<<"time">>]) when is_tuple(Obj) ->
        element(3, Obj);
    extract(Obj, [<<"weather">>]) when is_tuple(Obj) ->
        element(4, Obj);
    extract(Obj, [<<"temperature">>]) when is_tuple(Obj) ->
        element(5, Obj).
    
    get_field_type([<<"geohash">>]) -> varchar;
    get_field_type([<<"user">>]) -> varchar;
    get_field_type([<<"time">>]) -> timestamp;
    get_field_type([<<"weather">>]) -> varchar;
    get_field_type([<<"temperature">>]) -> varchar.
    
    get_field_position([<<"geohash">>]) -> 1;
    get_field_position([<<"user">>]) -> 2;
    get_field_position([<<"time">>]) -> 3;
    get_field_position([<<"weather">>]) -> 4;
    get_field_position([<<"temperature">>]) -> 5.

    get_field_positions() -> [
        {[<<"geohash">>], 1},
        {[<<"user">>], 2},
        {[<<"time">>], 3},
        {[<<"weather">>], 4},
        {[<<"temperature">>], 5}].

    is_field_valid([<<"geohash">>]) -> true;
    is_field_valid([<<"user">>]) -> true;
    is_field_valid([<<"time">>]) -> true;
    is_field_valid([<<"weather">>]) -> true;
    is_field_valid([<<"temperature">>]) -> true;
    is_field_valid([<<"*">>]) -> true;
    is_field_valid(_) -> false.
    
    get_ddl() ->
        {ddl_v1, <<"timeseries_filter_test">>,
         [{riak_field_v1, <<"geohash">>, 1, varchar, false},
          {riak_field_v1, <<"user">>, 2, varchar, false},
          {riak_field_v1, <<"time">>, 3, timestamp, false},
          {riak_field_v1, <<"weather">>, 4, varchar, false},
          {riak_field_v1, <<"temperature">>, 5, varchar, true}],
         {key_v1,
          [{hash_fn_v1, riak_ql_quanta, quantum,
    	[{param_v1, [<<"time">>]}, 15, s], undefined}]},
         {key_v1,
          [{param_v1, [<<"time">>]}, {param_v1, [<<"user">>]}]}}.
    
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
