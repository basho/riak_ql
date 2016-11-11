%% -------------------------------------------------------------------
%%
%% riak_ql_pfitting: Riak Query Pipeline pipe fitting (pfitting) behaviour.
%%
%% @doc Riak Query Pipeline pipe fitting (pfitting) behaviour and
%% supporting record types and accessors used in processing multiple
%% row result sets.
%%
%% Example fittings developed and planned follow:
%% <ul>
%%   <li>Rows within result set modification, ie aggregate reducing
%%       row count.</li>
%%   <li>Columns within Row modification, ie projection and
%%       arithmetic.</li>
%%   <li>[TODO] Joining of result sets, ie inner, left (inner + diff),
%%       or full.</li>
%%   <li>[TODO] Filtering, reducing rows to only those that match a
%%       predicate.</li>
%%   <li>[TODO] Sorting</li>
%% </ul>
%%
%% Stretch cases include:
%% <ul>
%%   <li>[TODO] Distributed aggregating, ie AVERAGE(field), which is a
%%       chain of aggregating at a scatter node to a cummatative result
%%       set, joining at a gather node, then aggregating.</li>
%%   <li>[TODO] Grouping, a chain of filtering, sorting, post-filtering,
%%       then aggregating.</li>
%% </ul>
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
-module(riak_ql_pfitting).

-record(pfitting, {
          column_mappings :: [riak_ql_pfitting_column_mapping:column_mapping()]
         }).
-opaque pfitting() :: #pfitting{}.
-export_type([pfitting/0]).

-export([create/1,
         get_column_mappings/1]).

-callback process(pfitting(), [binary()], [[term()]]) ->
    {ok|error, riak_ql_pfitting_process_result:process_result()}.

-spec create([riak_ql_pfitting_column_mapping:column_mapping()]) -> pfitting().
create(ColumnMappings) ->
    #pfitting{column_mappings = ColumnMappings}.

-spec get_column_mappings(pfitting()) -> [iriak_ql_pfitting_column_mapping:column_mapping()].
get_column_mappings(#pfitting{column_mappings = ColumnMappings}) -> ColumnMappings.
