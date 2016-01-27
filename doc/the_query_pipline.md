# The Query Pipeline

## Introduction

This section describes:
- why the `riak_sql_select_v1{}` record is structured like it is
- how it is processed in the lexer-parser, in particular:
  - how (and why) different clauses are canonicalised (or not)
  - how, why and when it it validated
- how it is processed in the query sub-system
  - rewriting to be a set of time-series queries
  - transformed at the vnode into a leveldb friendly filter


## The `riak_sql_select_v1{}` Record

The important thing about the  `riak_sql_select_v1{}` record is that it takes many forms. It contains a number of fields which are semantically consistent but which have implementation differences. SQL is a declarative language and the SQL clause structure is preservered, so fields like `SELECT`, `FROM`, `WHERE`, `ORDER BY` and `LIMIT` may contain different data structures for the purposes of execution but which carry the same sematic burden.

It would have been possible to have each stage of the pipeline have its own record - and this seems sensible in Time Series when there are basically 2 major types (`sql` and `timeseries`) but already there are several unimplemented ones shadowly emerging on the road map (eg 'TS full table scan', 'composite key read'). The Spark connector integration could easily be constructed as a new record. This approach would lead to a lot of different records with very similar names and structures.
