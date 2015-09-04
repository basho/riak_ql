%% -------------------------------------------------------------------
%%
%% riak_kv_sql: defines records used in the data description language
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

-ifndef(RIAK_QL_SQL_HRL).
-define(RIAK_QL_SQL_HRL, included).

%% TODO these types will be improved over the duration of the time series project
-type selection()  :: [binary()].
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
	  partition_key = none  :: none | binary(),
	  is_executable = false :: boolean(),
	  type          = sql   :: sql | timeseries,
	  local_key                                  % prolly a mistake to put this here - should be in DDL
	}).

-endif.
