%% -------------------------------------------------------------------
%%
%% riak_index: central module for indexing.
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% TODO these types will be improved over the duration of the time series project
-type selection()  :: term().
-type filter()     :: term().
-type operator()   :: term().
-type sorter()     :: term().
-type combinator() :: term().
-type limit()      :: any().

-record(riak_ql_li_index_v1, {
	  bucket        = <<>>  :: binary(),
	  partition_key = <<>>  :: binary(),
	  is_executable = false :: boolean(),
	  selections    = []    :: [selection()],
	  filters       = []    :: [filter()],
	  operators     = []    :: [operator()],
	  sorters       = []    :: [sorter()],
	  combinators   = []    :: [combinator()],
	  limit         = none  :: limit()
	 }).

-define(KV_QL_INDEX_Q, #riak_ql_li_index_v1).
