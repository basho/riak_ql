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

%% @doc This document implements the riak_ql SQL Insert clause 
%% in the query pipeline
-module(riak_ql_insert_pipeline).

-export([
         make_insert/3
        ]).

make_insert({identifier, Table}, Fields, Values) ->
    FieldsAsList = case is_list(Fields) of
                       true  -> Fields;
                       false -> []
                   end,
    FieldsWrappedIdentifiers = [riak_ql_parser_util:wrap_identifier(X) || X <- FieldsAsList],
    {insert, [
              {table, Table},
              {fields, FieldsWrappedIdentifiers},
              {values, Values}
             ]}.
