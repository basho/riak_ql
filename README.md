# Riak QL

## 1 Introduction

riak_ql provides a number of different functions to Riak
* a lexer/parser for the SQL sub-set we support
* a function for calculating time quanta for Time Series
* a compiler for generating helper modules to validate and manipulate records that correspond to a defined table schema in the DDL

Link to the official and still private [docs](https://github.com/basho/private_basho_docs/tree/timeseries/1.0.0/source/languages/en/riakts).

This README is an overview of the repo individual sub-systems have their own documentation which will be linked to as appropriate.

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

For more details of the lexer and parser see the [Lexer And Parser](./doc/lexer_parser.md)

To understand how the lexer/parser fits into the query pipeline see [Query Pipeline](./doc/the_query_pipeline.md)

There is a ruby script and set of SQL keywords which can be used to generate some of the lexer components of `riak_lexer.xrl`:
* `priv/keyword_general.rb`
* `priv/riak_ql_keywords.csv`

For more details see the [Lexer Keywords](./doc/lexer_keywords.md)

This code generates one of 2 output records:
* `ddl_v1{}` - which captures the `CREATE TABLE...` statement
* `riak_select_v1{}` - which captures a `SELECT * FROM...` statement

### 4.iii Time Quantiser

Time quantisation is done by the module:
* `riak_ql_quanta.erl`

Please read the inline documentation for this module.

### 4.iv DDL Compiler

The DDL compiler is implemented by:
* `riak_ql_ddl_compiler.erl`
* `riak_ql_ddl.erl`

When a `CREATE TABLE...` statement if fed into the lexer/parser it generates a `#ddl_v1{}` - a data description language. This captures the structure of the table. The DDL compiler then generates a helper module for that table which allows for the programatic manipulation of data in that particular format via a common and fixed API.

The module `riak_ql_ddl_compiler.erl` performs the compilation and the module `riak_ql_ddl.erl` provides a set of wrappers around the compiled module to add utility functions to the API.

For more details see the [DDL Compiler](./doc/ddl_compiler.md)


### 4.v Runtime Query Fns

The runtime query system performs operations on data in the query pipeline by calling a set of library functions. These are defined in:
* `riak_ql_window_agg_fns.erl`


