# The Lexer-Parser

## 1 Overview

The query system uses declarative SQL-like language to instruct riak to behave in certain ways.

Wherever possible it uses pure SQL as defined in the SQL spec - if there is variation it has a prejudice to being compatible with PostgreSQL.

The specification used can be found in our Google Docs folders:
https://drive.google.com/drive/u/0/folders/0B-2H_9GJ40JpUGZld19GTHB3b2M

To this end non-terminals in the parser MUST follow the structure of the SQL spec.

The SQL statements are written as strings and are compiled first to proplists which are then converted to erlang records as shown:

```
        String           Tokens            Erlang
                                           Records

Client +--------> Lexer +--------> Parser +----------> proplist -> #ddl_v1{}
                                          | CREATE
                                          | TABLE
                                          |
                                          |
                                          +----------> proplist -> #riak_ql_select_v1{}
                                          | SELECT
                                          | FROM
                                          |
                                          |
                                          +----------> proplist -> #riak_sql_decribe_v1{}
                                            DESCRIBE
```

At the moment 3 types of statement are supported:
- `CREATE TABLE` statements (these are not standard SQL)
- SQL `SELECT FROM` queries (these conform to a valid sub-set of SQL)
- SQL `DESCRIBE` queries (these conform to a valid sub-set of SQL)

The lexer/parser functions are generated using the tools `leex` and `yecc` (the erlang equivalents of `lex` and `yacc`) from the language definition files `riak_ql_lexer.xrl` and `riak_ql_parser.yrl`. `rebar` detects these files at run time - and leex/yeccs them into `riak_ql_lexer.erl` and `riak_ql_parser.erl` which it then compiles as normal.

These three data structures that are emitted are used in different ways.

**NOTE:** to make inter-repository dependencies easier to manage the lexer/parser in `riak_ql` emits proplists of key/value pairs which are poured into the relevant record on ingest to `riak_kv`.

## 2 Emitted data structures

### 2.a The `CREATE TABLE` Record

The create table function outputs a `ddl_v1{}` record which encapsulates the description of the table and is used by `riak_ql_ddl_compiler.erl` to generate a helper function which can be used against data that should conform to that definition. See the documentation for the [ddl compiler](./ddl_compiler.md)

One of the peculiarities of the data structures emitted by the lexer-parser is that 'field names' are emitted as 'paths' - that is the field `<<"myfield">>` is represented as `[<<"myfield">>]`. The reason for this is to enable future implementation of sets and maps - that is nested data structures. In a world without maps the name of a field is a degenerate path of length 1. If the top level construct was a map called `<<"mymap">>` which contained named fields `<<"field1">>`, `<<"field2>>"` etc, etc, these would be accessed by the paths that looked like `[<<"mymap">>, <<"field1">>]`.

### 2.b The `SELECT FROM` Record

The record `#riak_ql_select_v1{}` is the backbone of the query system. It goes through a set of transformations described in the documentation [the query pipeline](./the_query_pipeline.md). For ease of exposition the fields that correspond to the clauses of a SQL SELECT statement are in upper-class, eg 'SELECT', 'FROM', 'WHERE', 'ORDER BY' and 'LIMIT'.

Fields are emitted by the lexer/parser in a manner that is consonant with the path description in the section on the `CREATE TABLE` record.

The `#riak_ql_select_v1{}` is validated, checked and rewritten as described in the document the [Query Pipeline](./doc/the_query_pipeline.md)

### 2.c The `DESCRIBE` Record

The record `#riak_sql_describe{}` at the moment simply contains the name of the table to be described. Its processing is trivial: on being passed to the query system, the appropriate DDL is read and converted into a user-friendly form and returned to the user.

## 3 Design Principles

### 3.a Conformity

There are two prejudices that inform development of the SQL toolchain in `riak_ql`:
* fidelity to the SQL spec
* in case of uncertainly be compatible with PostgreSQL

### 3.b Lisp-like

Various intermediate data structures emitted by the lexer/parser (particularly for `SELECT` queries) are YASL, or Lisp-like. As a general design rule we are moving towards a cleaner Lisp representation.

## 4 TODO/Outstanding Questions

* how deep do we want to go with the lexer/parser?
* do we want to describe the various components?