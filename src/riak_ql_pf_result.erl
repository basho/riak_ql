%% -------------------------------------------------------------------
%%
%% riak_ql_pf_result: Riak Query Pipeline pipe fitting
%% (pfitting) process result datatype used in returning a pfitting's
%% process result.
%%
%% Copyright (c) 2016-2017 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_ql_pf_result).

-type columns() :: [binary()].
-type rows() :: [[term()]].
-type errors() :: [atom() | {atom(), term()}].
-type status() :: ok | error.
-record(process_result, {
          columns :: columns(),
          rows :: rows(),
          errors :: errors()
         }).
-opaque process_result() :: #process_result{}.
-export_type([
    columns/0,
    errors/0,
    process_result/0,
    rows/0,
    status/0]).

-export([create/0,
         create/3,
         get_columns/1,
         get_rows/1,
         get_errors/1,
         get_status/1]).

-spec create() -> process_result().
create() ->
    #process_result{}.

-spec create(columns(), rows(), errors()) -> process_result().
create(Columns, Rows, Errors) ->
    #process_result{columns = Columns,
                    rows = Rows,
                    errors = Errors}.

-spec get_columns(process_result()) -> columns().
get_columns(#process_result{columns = Columns}) -> Columns.

-spec get_rows(process_result()) -> rows().
get_rows(#process_result{rows = Rows}) -> Rows.

-spec get_errors(process_result()) -> errors().
get_errors(#process_result{errors = Errors}) -> Errors.

-spec get_status(process_result()) -> status().
get_status(#process_result{errors=[]}) -> ok;
get_status(_) -> error.
