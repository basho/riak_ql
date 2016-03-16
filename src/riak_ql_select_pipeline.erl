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

%% @doc This document implements the riak_ql SQL Select clause 
%% in the query pipeline
-module(riak_ql_select_pipeline).

-export([
         make_select/2,
         get_version/0
]).

%%
%% API
%%

make_select({Select,
             {Type,       D},
             {_WhereType, E}}, Limit) ->
    Bucket = case Type of
                 identifier -> D;
                 list   -> {list, [X || X <- D]};
                 regex  -> {regex, D}
             end,
    FieldsAsList = case is_list(Select) of
                       true  -> Select;
                       false -> [Select]
                   end,
    FieldsWithoutExprs = [riak_ql_parser_util:remove_exprs(X) || X <- FieldsAsList],
    FieldsWrappedIdentifiers = [riak_ql_parser_util:wrap_identifier(X) || X <- FieldsWithoutExprs],
    L = case Limit of 
            none         -> [];
            {integer, N} -> [{limit, N}]
        end, 
    [
     {type,   select},
     {fields, FieldsWrappedIdentifiers},
     {tables, Bucket},
     {where,  E}
    ] ++ L.

get_version() -> "1.3".

%%
%% Internal Fns
%%
