# The Query Pipeline

## 1	Introduction

This documenation describes:
* Section 2 - client-to-query processing
* Section 3 - query validation before execution
* Section 4 - query rewriting for execution


## 2 Client-to-Query Processing

This section shows the lifecyle of an SQL `SELECT` request

### 2.a Process Overview

```

Client-side   +--+
text query       |  Protocol
                 |  Buffers
                 +------------> Lexer +---> Parser +----> Canonicaliser +-----> Proplist
                 |                                         (in Parser)
Client-side      |
parameterised +--+
query

```

### 2.b Conformance with the SQL Foundation Document

It is worthing making the point about conformance with the spec at this stage. Below is an extract from the riak_ql_parser.yrl language definition of a Numeric Value Expression:

```erlang
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

     (...)
```

This is the corresponsing section of the SQL Foundation Document:

![SQL Foundation Document](./img/SQL_Foundation_Doc.png)

The SQL Foundation Document can actually be transcribed directly into the riak SQL parser - from whence it determines the structure of the riak SQL lexer as shown in the extract below:

```erlang
PLUS     = (\+)
MINUS    = (\-)
ASTERISK = (\*)
SOLIDUS  = (/)
```

### 2.c The Role Of Canonicalisation

The lexer/parser output is canonicalised in a number of ways - this is to ensure that SQL statements that are logically *equivalent* have an *identical* (not merely *equivalent*) output.

A trivial example of this is keyword canonicalisation in the lexer - which will be discussed here for expository purposes. Consider the following *equivalent* SQL statements

```sql
SELECT * FROM mytable;
SelECt * frOm mytable;
select * from mytable;
```

The lexer canonicalises this trivially in the regex's that define the SQL keyword tokens:
```erlang
QUANTUM  = (Q|q)(U|u)(A|a)(N|n)(T|t)(U|u)(M|m)
SELECT   = (S|s)(E|e)(L|l)(E|e)(C|c)(T|t)
DESCRIBE = (D|d)(E|e)(S|s)(C|c)(R|r)(I|i)(B|b)(E|e)
```

The important canonicalisation (which is implemented in the parser) is the canonicalisation of the `WHERE` clause. Consider the following equivalent statements:
```
SELECT * FROM mytable WHERE field1 >= 10 AND (field2 < 2 OR field3 = 'alice');
SELECT * FROM mytable WHERE  (field3 = 'alice' or field2 < 2) AND field1 >= 10;
```

The `WHERE` is a specialist mini-lisp being a tree whose leaves consist of operators that return `booleans`:
```erlang
{'>=', <<"field1">>, {integer, 10}}
{'<',  <<"field2">>, {integer, 2}}
{'=',  <<"field3">>, {binary,  <<"alice">>}}
```

which can be composed with the standard set of `boolean` operators:
```erlang
{or,  EXPR1, EXPR2}
{and, EXPR1, EXPR2}
```

The `WHERE` clause is canonicalised here - with a particular eye on simplifying the query compiler. At this stage the SQL statement is still *declatory* - it signals the intent to retrieve data - but it is not yet *executable*.

**TODO:** break the canconicaliser out into its own module

### 2.d Proplist Output

By emitting a proplist (or set of nested proplists) whose key equate to record fields. This is to decouple dependencies on `.hrl` files between `riak_kv` and `riak_ql` and make multi-repo releases less of a pain in the neck to wrestle out of the door.

## 3 Query Validation Before Execution

### 3.a Overview

The output of the lexer/parser is a valid SQL statement - but that doesn't not correspond to a valid query. There are three different types of validation that the query must pass before it is executable:

* do the fields in various query clauses correspond to fields in the underlying declared table defined by a `CREATE TABLE` statement? The clauses that must be checked are:
  * `SELECT`
  * `WHERE`
  * (`ORDER BY` - not implemented yet)
  * (`GROUP BY` - not implemented yet)
* TS can only run a subset of queries at the moment
  * 'the `WHERE` clause of the query must cover the partition and local keys'
* are any arithmetic operations in the `SELECT` (or the other as-yet-unimplemented clauses) type safe?
  * for instance you can't divide `varchars` by `integers`

```
                      DDL                DDL           DDL      Function
                     Helper             Helper        Helper    Type Sigs
                       +                  +             +           +
                       |                  |             +-----+-----+
                       |                  |                   |
                       |                  |                   |
                       v                  v                   v
Declatory          Are fields Yes    Is valid TS    Yes    Is query    Yes   To Query
SQL Select  +----> in table? +-----> WHERE clause? +-----> typesafe? +-----> Rewriter
Record                 +                  +                   +
                    No |               No |                No |
                       |                  |                   |
                       v                  v                   v
                     Error              Error               Error

```

Query validation happens in `riak_kv` after the query is handed over to be executed

In future the restriction on *key covering* queries will be relaxed but other validation will be required - for instance when the `AS` keyword which implements aliasing is added, or when multi-table inputs (with the requirement for column name disambiguation, etc) is allowed.

The DDL helper functions emitted by the DDL Compiler are key here - see the [DDL Compiler](./ddl_compiler.md)

### 3.b Valid Fields

This stage of the validation is straightforward, iterate over the Lisp outputs that corrspond to the `SELECT` and `WHERE` clauses and for every leaf that is a field reference check that its name corresponds to a declared field.

### 3.c Time Series Key Restrictions

The key restrictions are enforced with the BFL methodology - *brute force and ignorance*. `WHERE` clauses that completley cover the key space are accepted, the rest are errored.

### 3.d Typesafe Arithmetic

This is similar to the first validation - the Lisp of the select clause needs to be iterated over - and two checks performed:

* is the application of a function to a field valid?
  * for instance you **CAN** `COUNT` a `sint64` or a `double` or a `varchar`
  * for instance you **CANNOT** `AVG` a `varchar`
* how do the types compose up an arithmetic structure?

The validator requires information from the DDL helper module as well as the type specs from modules that implement the functions (eg `COUNT`, `AVG`, `SUM`) as well as arithmetic operators (`+`, `-`, `/` or `*`) to perform this validation.

**NOTE:** wherever possible functions that validate queries should be generated directly into the helper modules - they are fast, efficient, testable and behave predicatably with different underlying data schemas

### 3.e Output

The SQL query is not changed by the validation process. A given query is either valid - in which case it passed on to the query sub-system for execution - or it is invalid - in which case an appropriate error message is sent back to the user who submitted it.

## 4 Query Rewriting For Execution

## XX The `riak_sql_select_v1{}` Record

The important thing about the  `riak_sql_select_v1{}` record is that it takes many forms. It contains a number of fields which are semantically consistent but which have implementation differences. SQL is a declarative language and the SQL clause structure is preservered, so fields like `SELECT`, `FROM`, `WHERE`, `ORDER BY` and `LIMIT` may contain different data structures for the purposes of execution but which carry the same sematic burden.

It would have been possible to have each stage of the pipeline have its own record - and this seems sensible in Time Series when there are basically 2 major types (`sql` and `timeseries`) but already there are several unimplemented ones shadowly emerging on the road map (eg `TS full table scan`, `composite key read`). The Spark connector integration could easily be constructed as a new record. This approach would lead to a lot of different records with very similar names and structures.
