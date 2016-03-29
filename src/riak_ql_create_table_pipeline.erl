%% -------------------------------------------------------------------
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

%% @doc This document implements the riak_ql SQL Create Table clause 
%% in the query pipeline
-module(riak_ql_create_table_pipeline).

-export([
         make_create_table/2
]).

-include("riak_ql_ddl.hrl").

%%
%% API 
%%

make_create_table({identifier, Table}, Contents) ->
    DDL = #ddl_v1{
             table         = Table,
             partition_key = find_partition_key(Contents),
             local_key     = find_local_key(Contents),
             fields        = find_fields(Contents)},
    riak_ql_ddl_validate_pipeline:validate_ddl(DDL).

%%
%% Internal Fns
%%
find_partition_key({table_element_list, Elements}) ->
    find_partition_key(Elements);
find_partition_key([{partition_key, Key} | _Rest]) ->
    Key;
find_partition_key([_Head | Rest]) ->
    find_partition_key(Rest);
find_partition_key(_) ->
    none.

find_fields({table_element_list, Elements}) ->
    find_fields(1, Elements, []).

find_fields(_Count, [], Found) ->
    lists:reverse(Found);
find_fields(Count, [Field = #riak_field_v1{} | Rest], Elements) ->
    PositionedField = Field#riak_field_v1{position = Count},
    find_fields(Count + 1, Rest, [PositionedField | Elements]);
find_fields(Count, [_Head | Rest], Elements) ->
    find_fields(Count, Rest, Elements).

find_local_key({table_element_list, Elements}) ->
    find_local_key(Elements);
find_local_key([{local_key, Key} | _Rest]) ->
    Key;
find_local_key([_Head | Rest]) ->
    find_local_key(Rest);
find_local_key(_) ->
    none.

