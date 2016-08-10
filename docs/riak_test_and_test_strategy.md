# Testing The Query System

This document can be displayed as a presentation with:
http://remarkjs.com/remarkise

---

## Overview

The testing strategy for the query system follows a tiered-approach.

The purpose of this is to enable developers to be 'slapdash' in their changes secure in the knowledge that:
* there is great regression coverage
* the test suite will triage bugs for them and inform them in what order to fix bugs

In addition `riak-shell` has a quick-regression capability.

---

## Testing _Layers_

There are six layers of tests, which should be fixed in descending order:

1 plain `eunit` tests
* simple
* QuickCheck

2 `ts_simple_*` tests in `riak_test`

3 `ts_cluster_*` tests in `riak_test`

4 `ts_degraded_*` tests in `riak_test`

5 `ts_*_eqc` QuickCheck tests running under `riak_test` (these are still under development)
* 'good' input tests based on generating valid inputs
* 'bad' input tests based on generating invalid inputs

6 `ts_updgrade_downgrade_*_eqc` upgrade/downgrade QuickCheck tests running under `riak_test` (these are still under development)

---

## Test Types

Unit tests run against individual modules

The `ts_simple_*` tests run against a cluster of 1 node.

The `ts_cluster_*` tests run against a full cluster.

The `ts_degraded_*` tests run against a cluster with at least 1 member taken down.

The theory is that bugs that a test suite checks for cast shadows down the suite - so if there is a unit test failing in 1 - then it should appear as failures in some or all of 2, 3, 4, 5 and 6.

The main Quick Check tests are the backstop.

---

## Self Triaging Test Suite

The 'slapdash' approach is make a set of changes and then:

* get a bloodbath of failing tests
* fix the type 1 test fails
* retest
* fix the type 2 test fails
* retest
* rinse, repeat up the ladder.

To help with this approach TS also uses the standard Erlang Common Test (`ct`) framework. This allows for more granular testing and better reporting.

---

## Common Test and `riak_test`

There are a number of misconceptions about the relationship of `riak_test` and `ct`.

`riak_test` is a large and powerful set of libraries for manipulating riak clusters - including intercept facilities for inserting code shims into running riak nodes and instrumenting sub-systems for testing purposes. `riak_test` also includes an underpowered and fairly primitive test runner.

`ct` is an industrial-strength battle hardened test runner for handling tens and hundreds of thousands of tests, distributed over dozens of test machines. `ct` knows less than nothing about riak and cares even less.

In Time Series we have the `riak_test` runner invoke suites of `ct` tests and let `ct` handle the orchestration. Individual tests in the `ct` suite use the various powerful `riak_test` library functions to manipulate the riak cluster under test as appropriate.

---

## Relationship Of `ct` To `riak_test`

This is a Venn Diagram of the overlap:

```
+------------------------------------+      +-------------------------------------+
|                                    |      |                                     |
|                  +-------------+   |      |                                     |
| Common Test      |             |   |      | Riak Test                           |
|                  | Giddy Up    |   |      | intercept and cluster manipulation  |
|                  |             |   |      | libraries                           |
|                  +-------------+   |      |                                     |
|                                    |      |                                     |
|                  +-------------+   |      |                                     |
|                  |             |   |      |                                     |
|                  |  riak_test  |   |      |                                     |
|                  | test runner |   |      |                                     |
|                  |             |   |      |                                     |
|                  +-------------+   |      |                                     |
|                                    |      |                                     |
+------------------------------------+      +-------------------------------------+
```

---

## `riak-shell` And `riak_test`

`riak_shell` can capture user logs which can be fed back into `riak_shell` in either **replay** or **regression** mode.

Details of how to capture these logs is given in the `riak_shell` README:
https://github.com/basho/riak_shell/blob/riak_ts-develop/README.md

These regression logs can be productionised in `riak_test`. For an example see:
https://github.com/basho/riak_test/blob/riak_ts-develop/tests/ts_cluster_riak_shell_regression_log.erl

---

# Fin