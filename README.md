# Riak QL

## 1 Introduction

riak_ql provides a number of different functions to Riak
* a lexer/parser for the SQL sub-set we support
* a function for calculating time quanta for Time Series
* a compiler for generating helper modules to validate and manipulate records that correspond to a defined table schema in the DDL

Link to the official and still private [docs](https://github.com/basho/private_basho_docs/tree/timeseries/1.0.0/source/languages/en/riakts).

## 2 Table Of Contents

This document contains the following sections (marked as to their completeness)
- [X] 1 Introduction
- [ ] 2 Table Of Contents
- [X] 3 Repository Contents
- [X] 4 Summary
   - [X] 4.i Runtime Tools
   - [X] 4.ii SQL Lexer/Parse
   - [X] 4.iii Time Quantiser Fn
   - [X] 4.iv DDL Compiler
   - [X] 4.v Runtime Query Fns
- [ ] 5 The Query Toolchain
   - [ ] 5.i Introduction
   - [ ] 5.ii Compiling and Decompiling
   - [ ] 5.iii Generated DDL Modules

## 3 Repository Contents

This application contains the following files:
* `riak_ql_cmd.erl`
* `riak_ql_ddl_compiler.erl`
* `riak_ql_ddl.erl`
* `riak_ql_lexer.xrl`
* `riak_ql_parser.yrl`
* `riak_ql_quanta.erl`
* `riak_ql_to_string.erl`
* `riak_ql_window_agg_fns.erl`

These are summarised in the following sections:
* [4.i Runtime Tools](##4i-runtime-tools)
* [4.ii SQL Lexer/Parser](##4ii-sql-lexer-parser)
* [4.iii Time Quantiser Fn](##4iii-time-quantiser)
* [4.iv DDL Compiler](##4iv-ddl-compiler)
* [4.v Runtime Query Fns](##4v-runtime-query-fns)

In depths discussion is in Section 5.

----

## 4 Summary

### 4.i Runtime Tools

There is an escript that lets you run the lexer/parser from the command line - it is designed to help developers/operators check their syntax, etc, etc
* `riak_ql_cmd.erl`

Please read the inline documentation for this module.

### 4.ii SQL Lexer/Parser

The SQL Lexer/Parser is takes a string representation of a SQL query and then compiles. The modules that perform this are:
* `riak_ql_lexer.xrl`
* `riak_ql_parser.yrl`

Running `./rebar compile` transforms this pair of leex and yecc files into the executable Erlang counterparts:
* `riak_ql_lexer.erl`
* `riak_ql_parser.erl`

There is a ruby script and set of SQL keywords which can be used to generate some of the lexer components of `riak_lexer.xrl`:
* `priv/keyword_general.rb`
* `priv/riak_ql_keywords.csv`

To add new keywords to the lexer:

1. Add them to the `priv/riak_ql_keywords.csv` file, one per line. Keeping this file in alphabetic order simplifies future changes.
2. Run `ruby priv/keyword_generator.rb` with Ruby 1.9.3 or newer.
3. Replace the keyword definitions near the top of `src/riak_ql_lexer.xrl` with the regex definitions (first chunk of output) from the message generator.
4. Replace the keyword rules in the `src/riak_ql_lexer.xrl` file with the second chunk of output from the message generator.
5. Save and commit the csv and lexer changes.

If this is done correctly, the commit diff should simply be a few lines added to the csv and a few lines added to the lexer.

The full operation of this system is explained in the section [The Query Toolchain](###the-query-toolchain).

This code generates one of 2 output records:
* `ddl_v1{}` - which captures the `CREATE TABLE...` statement
* `riak_select_v1{}` - which captures a `SELECT * FROM...` statement

### 4.iii Time Quantiser

Time quantisation is done by the module:
* `riak_ql_quanta.erl`

Please read the inline documentation for this module.

## 4.iv DDL Compiler

The DDL compiler is implemented by:
* `riak_ql_ddl_compiler.erl`
* `riak_ql_ddl.erl`

When a `CREATE TABLE...` statement if fed into the lexer/parser it generates a `#ddl_v1{}` - a data description language. This captures the structure of the table. The DDL compiler then generates a helper module for that table which allows for the programatic manipulation of data in that particular format via a common and fixed API.

The module `riak_ql_ddl_compiler.erl` performs the compilation and the module `riak_ql_ddl.erl` provides a set of wrappers around the compiled module to add utility functions to the API.

For more details see the section [Generated DDL Modules](###generated-ddl-modules)

### 4.v Runtime Query Fns

The runtime query system performs operations on data in the query pipeline by calling a set of library functions. These are defined in:
* `riak_ql_window_agg_fns.erl`


----

## 5 The Query Toolchain

### 5.i Introduction

### 5.ii Compiling and Decompiling

```
Table_def = "CREATE TABLE MyTable (myfamily varchar not null, myseries varchar not null, time timestamp not null, weather varchar not null, temperature double, PRIMARY KEY ((myfamily, myseries, quantum(time, 10, 'm')), myfamily, myseries, time))".

{ok, DDL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def)).
{ModName, AST} = riak_ql_ddl_compiler:compile(DDL).

riak_ql_ddl_compiler:write_source_to_files("/tmp", DDL, AST).
```

### 5.iii Generated DDL Modules

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

