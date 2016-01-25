# The Lexer-Parser

The query system users declarative SQL-like language to instruct riak to behave in certain ways.

Wherever possible it uses pure SQL as defined in the SQL spec - if there is variation it has a prejudice to being compatible with PostgreSQL.

The specification used can be found in our Google Docs folders:
https://drive.google.com/drive/u/0/folders/0B-2H_9GJ40JpUGZld19GTHB3b2M

The SQL statements are written as strings and compiled to erlang records as shown:


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


At the moment 3 types of statement are supported:
- CREATE TABLE statements (these are not standard SQL)
- SQL Select Queries (these conform to a valid sub-set of SQL)
- SQL DESCRIBE Queries (these conform to a valid sub-set of SQL)

The lexer/parser functions are generated using the tools `leex` and `yecc` (the erlang equivalents of `lex` and `yacc`) from the language definition files `riak_ql_lexer.xrl` and `riak_ql_parser.yrl`. Rebar detects these files at run time - and leex/yeccs them into `riak_ql_lexer.erl` and `riak_ql_parser.erl` which it then compiles as normal.

These three data structures that are emitted are used in different ways.