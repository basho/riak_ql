%% -------------------------------------------------------------------
%%
%% riak_kv_ddl: defines records used in the data description language
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-ifndef(RIAK_QL_DDL_HRL).
-define(RIAK_QL_DDL_HRL, included).

-record(riak_field_v1, {
          name     = <<>>  :: binary(),
          position         :: undefined | pos_integer(),
          type             :: undefined | field_type(),
          optional = false :: boolean()
         }).

-type field_type()         :: simple_field_type() | complex_field_type().
-type simple_field_type()  :: varchar | sint64 | double | timestamp | boolean | set.
-type complex_field_type() :: {map, [#riak_field_v1{}]} | any().

%% Relational operators allowed in a where clause.
-type relational_op() :: '=' | '!=' | '>' | '<' | '<=' | '>='.

-record(param_v1, {
          name :: [binary()]
         }).

-record(hash_fn_v1, {
          mod       :: atom(),
          fn        :: atom(),
          args = [] :: [#param_v1{} | any()],
          type      :: field_type()
	 }).

-record(key_v1, {
          ast = [] :: [#hash_fn_v1{} | #param_v1{}]
         }).

-record(ddl_v1, {
          table              :: binary(),
          fields        = [] :: [#riak_field_v1{}],
          partition_key      :: #key_v1{} | none,
          local_key          :: #key_v1{}
         }).

%% TODO these types will be improved over the duration of the time series project
-type selection()  :: {identifier, [binary()]}.
-type filter()     :: term().
-type operator()   :: [binary()].
-type sorter()     :: term().
-type combinator() :: [binary()].
-type limit()      :: any().

-record(riak_sql_v1,
        {
          'SELECT'      = []    :: [selection() | operator() | combinator()],
          'FROM'        = <<>>  :: binary() | {list, [binary()]} | {regex, list()},
          'WHERE'       = []    :: [filter()],
          'ORDER BY'    = []    :: [sorter()],
          'LIMIT'       = []    :: [limit()],
          helper_mod            :: atom(),
          partition_key = none  :: none | #key_v1{},
          is_executable = false :: boolean(),
          type          = sql   :: sql | timeseries,
          cover_context = undefined :: term(), %% for parallel queries
          local_key                                  % prolly a mistake to put this here - should be in DDL
        }).

-endif.
