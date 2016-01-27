# The Lexer-Parser

The query system users declarative SQL-like language to instruct riak to behave in certain ways.

Wherever possible it uses pure SQL as defined in the SQL spec - if there is variation it has a prejudice to being compatible with PostgreSQL.

The specification used can be found in our Google Docs folders:
https://drive.google.com/drive/u/0/folders/0B-2H_9GJ40JpUGZld19GTHB3b2M

To this end non-terminals in the parser MUST follow the structure of the SQL spec.

The SQL statements are written as strings and compiled to erlang records as shown:

```
        String           Tokens            Erlang
                                           Records

Client +--------> Lexer +--------> Parser +----------> #ddl_v1{}
                                          | CREATE
                                          | TABLE
                                          |
                                          |
                                          +----------> #riak_ql_select_v1{}
                                          | SELECT
                                          | FROM
                                          |
                                          |
                                          +----------> #riak_sql_decribe_v1{}
                                            DESCRIBE
```

At the moment 3 types of statement are supported:
- `CREATE TABLE` statements (these are not standard SQL)
- SQL `SELECT FROM` Queries (these conform to a valid sub-set of SQL)
- SQL `DESCRIBE` Queries (these conform to a valid sub-set of SQL)

The lexer/parser functions are generated using the tools `leex` and `yecc` (the erlang equivalents of `lex` and `yacc`) from the language definition files `riak_ql_lexer.xrl` and `riak_ql_parser.yrl`. Rebar detects these files at run time - and leex/yeccs them into `riak_ql_lexer.erl` and `riak_ql_parser.erl` which it then compiles as normal.

These three data structures that are emitted are used in different ways.

## The `CREATE TABLE` Record

The create table function outputs a `#ddl{}` record which encapuslates the description of the table and is used by `riak_ql_ddl_compiler.erl` to generate a helper function which can be used against data that puportively conforms to that definition. See the documentation section entitled `ddl_compiler`

One of the peculiarites of the data structures emitted by the lexer-parser is that 'field names' are emitted as 'paths' - that is the field `<<"myfield">>` is represented as `[<<"myfield">>]`. The reason for this is to enable future implementation of sets and maps - that is nested data structures. In a world without maps the name of a field is a degenerate path of length 1. If the top level construct was a map called `<<"mymap">>` which contained named fields `<<"field1">>`, `<<"field2>>"` etc, etc, these would be accessed by the paths that looked like `[<<"mymap">>, <<"field1">>]`.

## The `SELECT FROM` Record

The record `#riak_ql_select_v1{}` is the backbone of the query system. It goes through a set of transformations described in the documentation 'the_query_pipeline'. For ease of exposition the fields that correspond to the clauses of a SQL SELECT statement are in upper-class, eg 'SELECT', 'FROM', 'WHERE', 'ORDER BY' and 'LIMIT'.

Fields are emitted by the lexer/parser in a manner that is consonant with the path description in the section on the `CREATE TABLE` record.

The `riak_ql_select_v1{}` is validated, checked and rewritten as described in the document `sql_query_pipeline`.

## The `DESCRIBE` Record

The record `riak_sql_describe{}` at the moment simply contains the name of the table to be described. Its processing is trivial, on being passed to the query system, the approriate DDL is read and converted into a human friendly form and returned to the user.