%% -------------------------------------------------------------------
%%
%% riak_ql_ddl.hrl: defines records used in the data description language
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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

%% NOTE: Every time there is a change to the DDL helper
%% or anything related to DDL changes, this number must be
%% incremented.  It is independent of the DDL record version (below).
-define(RIAK_QL_DDL_COMPILER_VERSION, 2).

-record(riak_field_v1, {
          name     = <<>>  :: binary(),
          position         :: undefined | pos_integer(),
          type             :: undefined | riak_ql_ddl:simple_field_type(),
          optional = false :: boolean()
         }).


-record(param_v1, {
          name = [<<>>] :: [binary()]
         }).

-record(hash_fn_v1, {
          mod       :: atom(),
          fn        :: atom(),
          args = [] :: [#param_v1{} | any()],
          type      :: riak_ql_ddl:simple_field_type()
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

-define(DDL, #ddl_v1).
-define(DDL_RECORD_NAME, ddl_v1).
-define(DDL_RECORD_VERSION, 1).

-endif.
