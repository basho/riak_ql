# The Query Pipeline

## Introduction

This documenation describes:

* Client-to-query processing
* Query validation before execution
* Query rewriting for execution

---

## Client-to-Query Processing

This section shows the lifecyle of an SQL `SELECT` request

### Client-to-Query Processing - Process Overview

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

### Client-to-Query Processing - Conformance with the SQL Foundation Document

It is worthing making the point about conformance with the spec at this stage. Below is an extract from the `riak_ql_parser.yrl` language definition of a Numeric Value Expression:

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

### Client-to-Query Processing - The Role Of Canonicalisation

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

### Client-to-Query Processing - Proplist Output

By emitting a proplist (or set of nested proplists) whose key equate to record fields. This is to decouple dependencies on `.hrl` files between `riak_kv` and `riak_ql` and make multi-repo releases less of a pain in the neck to wrestle out of the door.

---

## Query Validation Before Execution

### Query Validation Before Execution - Overview

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
                                    |
                      DDL                     DDL           DDL      Function
                     Helper         |        Helper        Helper    Type Sigs
                       +                       +             +           +
                       |            |          |             +-----+-----+
                       |                       |                   |
                       |            |          |                   |
                       v                       v                   v
Declatory          Are fields YES         Is valid TS    YES    Is query   YES    To Query
SQL Select  +----> in table? +----------> WHERE clause? +-----> typesafe? +-----> Rewriter
Record                 +                       +                   +
                    NO |                    NO |                NO |
                       |            |          |                   |
                       v                       v                   v
                     Error          |        Error               Error

                                    |

           Lexer/Parser - QL        |               Query Compiler KV
```

Query validation happens in `riak_kv` after the query is handed over to be executed

In future the restriction on *key covering* queries will be relaxed but other validation will be required - for instance when the `AS` keyword which implements aliasing is added, or when multi-table inputs (with the requirement for column name disambiguation, etc) is allowed.

The DDL helper functions emitted by the DDL Compiler are key here - see the [DDL Compiler](./ddl_compiler.md)

### Query Validation Before Execution - Valid Fields

This stage of the validation is straightforward, iterate over the Lisp outputs that corrspond to the `SELECT` and `WHERE` clauses and for every leaf that is a field reference check that its name corresponds to a declared field.

### Query Validation Before Execution - Time Series Key Restrictions

The key restrictions are enforced with the BFL methodology - *brute force and ignorance*. `WHERE` clauses that completley cover the key space are accepted, the rest are errored.

### Query Validation Before Execution - Typesafe Arithmetic

This is similar to the first validation - the Lisp of the `SELECT` clause needs to be iterated over - and two checks performed:

* is the application of a function to a field valid?
  * for instance you **CAN** `COUNT` a `sint64` or a `double` or a `varchar`
  * for instance you **CANNOT** `AVG` a `varchar`
* how do the types compose up an arithmetic structure?

The validator requires information from the DDL helper module as well as the type specs from modules that implement the functions (eg `COUNT`, `AVG`, `SUM`) as well as arithmetic operators (`+`, `-`, `/` or `*`) to perform this validation.

**NOTE:** wherever possible functions that validate queries should be generated directly into the helper modules - they are fast, efficient, testable and behave predicatably with different underlying data schemas

### Query Validation Before Execution - Output

The SQL query is not changed by the validation process. A given query is either valid - in which case it passed on to the query sub-system for execution - or it is invalid - in which case an appropriate error message is sent back to the user who submitted it.

---

## Query Rewriting For Execution

### Query Rewriting For Execution - Theory

The query rewriter can be thought of the in the following terms:

* the **semantic** meaning of the query remains unchanged
* the **syntactic** form is transformed
* the input is **declarative**
  * this is a statement of the results the users would like to see
* the output is **executable**
  * this is how Riak will return results that match the users declaration
* there is **NOT** a one-to-one correspondence between the **declerative** input and the **executable** output
  * the query compiler may, based on heuristics, put two queries with the same **declarative** form through different **executable** query plans that have different execution costs
* the data structure that describes a query can be made recursive by hoisting `SELECT`, `WHERE`, (not-yet-implemented) `GROUP BY` or (not-yet-implemented) `ORDER` clauses into the `FROM` clause and rewriting them

### Query Rewriting For Execution - The `riak_sql_select_v1{}` Record

```erlang
-record(riak_select_v1,
        {
          'SELECT'                  :: #riak_sel_clause_v1{},
          'FROM'        = <<>>      :: binary() | {list, [binary()]} | {regex, list()},
          'WHERE'       = []        :: [filter()],
          'ORDER BY'    = []        :: [sorter()],
          'LIMIT'       = []        :: [limit()],
          helper_mod                :: atom(),
          %% will include group by
          %% when we get that far
          partition_key = none      :: none | #key_v1{},
          %% indicates whether this 
          %% query has already been 
          %% compiled to a sub query
          is_executable = false     :: boolean(),
          type          = sql       :: sql | timeseries,
          cover_context = undefined :: term(), %% for parallel queries
          local_key                            %% prolly a mistake to put this here - should be in DDL
        }).
```

Notice that the fields in the record fall into 2 disctinct categories:

* **declarative** fields which contain the users intention
  * `SELECT`
  * `FROM`
  * `WHERE`
  * `ORDER BY`
  * `LIMIT`
* runtime information - required for **execution**
  * `helper_mod`
  * `is_executable`
  * `type`
  * `cover_context`
  * `local_key`

### Query Rewriting For Execution - Notes On Terminology

At the heart of thinking about query pipelines is the notion of **tables** which have **column descriptors** and **rows of data**. These are make visible by the riak-shell:

```
✅ riak_shell(27)>select time, weather, temperature from GeoCheckin where myfamily='family1' and myseries='seriesX' and time > 0 and time < 1000;
+----+----------------+---------------------------+
|time|    weather     |        temperature        |
+----+----------------+---------------------------+
| 1  |    z«êPò¹      |4.19111744258298777600e+18 |
| 2  |  ^OOgz^Blu7)   |6.07861409217513676800e+18 |
| 3  |      ÔÖã       |6.84034338181623808000e+17 |
| 4  |       ^G       |-5.55785206740398080000e+16|
| 5  |   ¸LËäà«d      |-3.62555783091625574400e+18|
| 6  |    ^AE^S¥      |1.11236574770119680000e+18 |
| 7  |    ïö?ï^Fv     |5.51455556936744140800e+18 |
| 8  | ^FtFVÅë=+#^Y5  |2.44525777392835584000e+17 |
| 9  |ðÁÖ·©Ü^GV^^^DkU |6.90864738609726668800e+18 |
| 10 | QÝZa^QËfQ      |5.08590022245487001600e+18 |
+----+----------------+---------------------------+
```

The columns in this table have *implicit type*, `time` is a `timestamp`, `weather` is a `varchar` and `temperature` is a `double`.

In discussing the query pipeline we regard column headers as one data structure and row sets as another and discuss how both of them may (or may not) be transformed in different contexts.

Conventionally we also consider SQL queries to unroll left to right:

```
single SQL statement written by user +----------------> many SQL fragments executed across the ring by riak
                                       query rewriter
```

Consequently the query pipeline operates right to left:

```
table returned to user <-----------+ table fragements created in pipeline
                         Operation
```

### Query Rewriting For Execution - Understanding The Query Pipeline

We can conceptualise the **declarative** statements as being logically related. Consider the following transforms (operating right to left, of course):

```
 Table In Shell                           Data On Disk

+-------+-------+                  +-------+-------+-------+
| ColX  | ColY  |                  | Col1  | Col2  | Col3  |
| Type1 | Type2 |                  | Type1 | Type2 | Type3 |
+-------+-------+    SQL Query     +-------+-------+-------+
                  <--------------+
+-------+-------+                  +-------+-------+-------+
| Val1X | Val1Y |                  | Val1a | Val1b | Val1c |
+---------------+                  +-----------------------+
| Val2X | Val2Y |                  | Val2a | Val2b | Val2c |
+-------+-------+                  +-----------------------+
                                   | Val3a | Val3b | Val3c |
                                   +-------+-------+-------+
```

`WHERE`, `ORDER BY` and `GROUP BY` are all row operations:

```
+-------+-------+-------+                 +-------+-------+-------+
| Col1  | Col2  | Col3  |                 | Col1  | Col2  | Col3  |
| Type1 | Type2 | Type3 |                 | Type1 | Type2 | Type3 |
+-------+-------+-------+    Operation    +-------+-------+-------+
                          <-------------+
+-------+-------+-------+                 +-------+-------+-------+
| Val1a | Val1b | Val1c |     WHERE       | Val1a | Val1b | Val1c |
+-----------------------+    GROUP BY     +-----------------------+
| Val3a | Val3b | Val3c |    ORDER BY     | Val2a | Val2b | Val2c |
+-----------------------+                 +-----------------------+
| Val6a | Val6b | Val6c |                 | Val3a | Val3b | Val3c |
+-----------------------+                 +-----------------------+
| Val5a | Val5b | Val5c |                 | Val4a | Val4b | Val4c |
+-------+-------+-------+                 +-----------------------+
                                          | Val5a | Val5b | Val5c |
                                          +-----------------------+
                                          | Val6a | Val6b | Val6c |
                                          +-------+-------+-------+
```

Row operations preserve **column names** and **column types**.

`SELECT` is **both** a column operator and a row operator:

```
+-------+-------+                 +-------+-------+-------+
| ColX  | ColY  |                 | Col1  | Col2  | Col3  |
| Type1 | Type2 |                 | Type1 | Type2 | Type3 |
+-------+-------+    Operation    +-------+-------+-------+
                  <-------------+
+-------+-------+                 +-------+-------+-------+
| Val1X | Val1Y |     SELECT      | Val1a | Val1b | Val1c |
+---------------+                 +-----------------------+
| Val2X | Val2Y |                 | Val2a | Val2b | Val2c |
+-------+-------+                 +-----------------------+
                                  | Val3a | Val3b | Val3c |
                                  +-------+-------+-------+
```

`SELECT` can transform both **column names** and **column types**.

You can see how and why this happens if you consider:

```sql
SELECT COUNT(myvarcharfield) AS bobcat FROM mytable;
SELECT COUNT(myintegerfield)/SUM(myintegerfield) FROM mytable;
```

**NOTE:** the `AS` keyword is not yet implemented.

In this context `FROM` can be considered to be the identity operator:

```
+-------+-------+-------+                 +-------+-------+-------+
| Col1  | Col2  | Col3  |                 | Col1  | Col2  | Col3  |
| Type1 | Type2 | Type3 |                 | Type1 | Type2 | Type3 |
+-------+-------+-------+    Operation    +-------+-------+-------+
                         <--------------+
+-------+-------+-------+                 +-------+-------+-------+
| Val1a | Val1b | Val1c |      FROM       | Val1a | Val1b | Val1c |
+-----------------------+                 +-----------------------+
| Val2a | Val2b | Val2c |                 | Val2a | Val2b | Val2c |
+-----------------------+                 +-----------------------+
| Val3a | Val3b | Val3c |                 | Val3a | Val3b | Val3c |
+-----------------------+                 +-----------------------+
| Val4a | Val4b | Val4c |                 | Val4a | Val4b | Val4c |
+-----------------------+                 +-----------------------+
| Val5a | Val5b | Val5c |                 | Val5a | Val5b | Val5c |
+-----------------------+                 +-----------------------+
| Val6a | Val6b | Val6c |                 | Val6a | Val6b | Val6c |
+-------+-------+-------+                 +-------+-------+-------+
```

`FROM` does 2 things - if it is right at the metal it is an identity operator where the right hand data is on disc and the left hand data is in memory. If it is executed at a higher level it just ingests data for one of the other operators to act on.

Using this knowledge we can trivially rewrite an SQL statement as a series of nested SQL statements - with the nesting being implemented as a logical pipeline.

Let us see this happening for a simple SQL statement:

```sql
SELECT, height, weight FROM mytable WHERE height > 10;
```

This can be rewritten into queries - the results of the right hand of which is operated on by the left hand query's `FROM` clause to ingest it:

**NOTE:** we have unrolled the query left-to-right, but this pipeline then executes right-to-left.

```
+ FROM     <---------------------+ + FROM     mytable
|                                  |
| SELECT   height, weight          | SELECT   *
|                                  |
| GROUP BY []                      | GROUP BY []
|                                  |
| ORDER BY []                      | ORDER BY []
|                                  |
+ WHERE    []                      + WHERE    height > 10
```

This process is trivially recursive. For operational reasons these operations must be chunked (to prevent memory overflow, unchuncked queries would simply load all the data in to memory, which kinda obviates the very existance of databases as a technology).

```
+-------+-------+                  +-------+-------+-------+
| ColX  | ColY  |                  | Col1  | Col2  | Col3  |
| Type1 | Type2 |                  | Type1 | Type2 | Type3 |
+-------+-------+    SQL Query     +-------+-------+-------+
                  <-----+
+-------+-------+       |          +-------+-------+-------+
| Val1X | Val1Y |       |          | Val1a | Val1b | Val1c |
+---------------+       | Chunk1   +-----------------------+
| Val4X | Val4Y |       +----------+ Val2a | Val2b | Val2c |
+-------+-------+       |          +-----------------------+
                        |          | Val3a | Val3b | Val3c |
                        |          +-------+-------+-------+
                        |
                        |
                        |          +-------+-------+-------+
                        |          | Val4a | Val4b | Val4c |
                        | Chunk2   +-----------------------+
                        +----------+ Val5a | Val5b | Val5c |
                        |          +-----------------------+
                        |          | Val6a | Val6b | Val6c |
                        |          +-------+-------+-------+
                        |
                        |
                        |          +-------+-------+-------+
                        |          | Val7a | Val7b | Val7c |
                        | Chunk3   +-----------------------+
                        +----------+ Val8a | Val8b | Val8c |
                                   +-----------------------+
                                   | Val9a | Val9b | Val0c |
                                   +-------+-------+-------+
```

The chunking may be for 2 reasons

* there is too much data from a particular vnode to appear in memory
* the data we want is on more than one vnode

Let us now rewrite a Time Series query to see something real:

```sql
SELECT device, temp FROM mytimeseries WHERE family = 'myfamily', series = 'myseries', timestamp > 1233
                                            and timestamp < 6789 and temp > 18;
```

This becomes (again executing right-to-left):

```
<----Erlang Coordinator----->        <-----LeveldDB C++ Code----->

                          <---Network--->

+ FROM     <----------------+        + FROM     mytable on vnode X
|                           |        |
| SELECT   device, temp     |        | SELECT   *
|                           | Chunk1 |
| GROUP BY []               +--------+ GROUP BY []
|                           |        |
| ORDER BY []               |        | ORDER BY []
|                           |        |
+ WHERE    []               |        + WHERE + start_key = {myfamily, myseries, 1233}
                            |                | end_key   = {myfamily, myseries, 4000}
                            |                + temp      > 18
                            |
                            |
                            |        + FROM     mytable on vnode Y
                            |        |
                            |        | SELECT   *
                            | Chunk2 |
                            +--------+ GROUP BY []
                                     |
                                     | ORDER BY []
                                     |
                                     + WHERE + start_key = {myfamily, myseries, 4001}
                                             | end_key   = {myfamily, myseries, 6789}
                                             + temp      > 18
```

Note 3 things:

* the `FROM` clauses are now no longer logical `FROM`s they are physical `FROM`s - a coverage plan has been constructed and these SQL statements have been dispatched to vnodes to run
* the `WHERE` clause has now be reweritten from a **declarative** one to a **executable** one - it has a leveldb start and end key to scan and then a clause to run on all values between those end points
* this diagram includes the physical locations the various pipeline functions run at - on the right hand side the C++ vnodes, on the left the Erlang co-ordinator (inside the query sub-system)

This is an accurate view of how Riak TS 1.2 would rewrite that statement. But there is an another obvious, semantically equivalent form that it could be rewritten to, and which would be more efficient (which will be implemented from Riak TS 1.3 onwards):

```
<----Erlang Coordinator----->        <-----LeveldDB C++ Code----->

                          <---Network--->

+ FROM     <----------------+        + FROM     mytable on vnode X
|                           |        |
| SELECT   *                |        | SELECT   device, temp
|                           | Chunk1 |
| GROUP BY []               +--------+ GROUP BY []
|                           |        |
| ORDER BY []               |        | ORDER BY []
|                           |        |
+ WHERE    []               |        + WHERE + start_key = {myfamily, myseries, 1233}
                            |                | end_key   = {myfamily, myseries, 4000}
                            |                + temp      > 18
                            |
                            |
                            |        + FROM     mytable on vnode Y
                            |        |
                            |        | SELECT   device, temp
                            | Chunk2 |
                            +--------+ GROUP BY []
                                     |
                                     | ORDER BY []
                                     |
                                     + WHERE + start_key = {myfamily, myseries, 4001}
                                             | end_key   = {myfamily, myseries, 6789}
                                             + temp      > 18
```

Performing the selection (which is a column reduction) at the vnode (the right hand side) means that less data is shipped around the cluster than in the case where the selection is performed at the left hand side (in the query co-ordinator). Imagine if the table `mytable` had 100 columns.

This the last essential operation of a query system, building a query plan. This is the most effecient way to rewrite this query to execute with this table which is loaded with data of this *shape* on a riak cluster with this many nodes and ring of this size.

Strategically the operations are undifferentiated between the Erlang co-ordinator and the leveldb C++ code. We can see why this is the general case if we rewrite the following SQL function:

```sql
SELECT AVG(temp) FROM mytimeseries WHERE family = 'myfamily', series = 'myseries', timestamp > 1233
                                         and timestamp < 6789 and temp > 18;
```

This becomes (again executing right-to-left):

```
<----Erlang Coordinator----->               <-----LeveldDB C++ Code----->

                               <---Network--->

+ FROM     <-----------------------+        + FROM     mytable on vnode X
|                                  |        |
| SELECT   SUM(STemp)/SUM(NoTemp)  |        | SELECT   SUM(temp) AS STemp, COUNT(temp) AS NoTemp
|                                  | Chunk1 |
| GROUP BY []                      +--------+ GROUP BY []
|                                  |        |
| ORDER BY []                      |        | ORDER BY []
|                                  |        |
+ WHERE    []                      |        + WHERE + start_key = {myfamily, myseries, 1233}
                                   |                | end_key   = {myfamily, myseries, 4000}
                                   |                + temp      > 18
                                   |
                                   |
                                   |        + FROM     mytable on vnode Y
                                   |        |
                                   |        | SELECT   SUM(temp) AS STemp, COUNT(temp) AS NoTemp
                                   | Chunk2 |
                                   +--------+ GROUP BY []
                                            |
                                            | ORDER BY []
                                            |
                                            + WHERE + start_key = {myfamily, myseries, 4001}
                                                    | end_key   = {myfamily, myseries, 6789}
                                                    + temp      > 18
```

Notice that the `AVG` function has been broken into a `SUM` and a `COUNT` to be run in the C++ code and another pair of `SUM`s to be run in the Erlang Node.

Strategically we wish to implement all operators in the pipeline (`FROM`, `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`) as an Erlang behaviour (or a behaviour-like construct in C++, a set of function callbacks with defined specs) and this behaviour MUST be identically callable from both the Erlang co-ordinator and within the C++ leveldb code:
```
+-------+-------+                  +-------+-------+-------+
| ColX  | ColY  |                  | Col1  | Col2  | Col3  |
| Type1 | Type2 |                  | Type1 | Type2 | Type3 |
+-------+-------+     Behaviour    +-------+-------+-------+
                  <--------------+
+-------+-------+                  +-------+-------+-------+
| Val1X | Val1Y |                  | Val1a | Val1b | Val1c |
+---------------+                  +-----------------------+
| Val2X | Val2Y |                  | Val2a | Val2b | Val2c |
+-------+-------+                  +-----------------------+
                                   | Val3a | Val3b | Val3c |
                                   +-------+-------+-------+
```


### Query Rewriting For Execution - Notes Of The Datastructure

The important thing about the  `riak_sql_select_v1{}` record is that it takes many forms. It contains a number of fields which are semantically consistent but which have implementation differences. SQL is a declarative language and the SQL clause structure is preservered, so fields like `SELECT`, `FROM`, `WHERE`, `ORDER BY` and `LIMIT` may contain different data structures for the purposes of execution but which carry the same sematic burden.

It would have been possible to have each stage of the pipeline have its own record - and this seems sensible in Time Series when there are basically 2 major types (`sql` and `timeseries`) but already there are several unimplemented ones shadowly emerging on the road map (eg `TS full table scan`, `composite key read`). The Spark connector integration could easily be constructed as a new record. This approach would lead to a lot of different records with very similar names and structures.

----

# FIN