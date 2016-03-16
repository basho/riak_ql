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
-module(riak_ql_ddl_validate_pipeline).

-export([
         validate_ddl/1,
         get_version/0
        ]).

-include("riak_ql_ddl.hrl").

%%
%% API 
%%
get_version() -> "1.3".

validate_ddl(DDL) ->
    ok = assert_keys_present(DDL),
    ok = assert_unique_fields_in_pk(DDL),
    ok = assert_partition_key_length(DDL),
    ok = assert_primary_and_local_keys_match(DDL),
    ok = assert_partition_key_fields_exist(DDL),
    ok = assert_primary_key_fields_non_null(DDL),
    DDL.

%% @doc Ensure DDL can haz keys
assert_keys_present(#ddl_v1{local_key = LK, partition_key = PK})
  when LK == none;
       PK == none ->
    riak_ql_parser:return_error_flat("Missing primary key");
assert_keys_present(_GoodDDL) ->
    ok.

%% @doc Ensure all fields appearing in PRIMARY KEY are not null.
assert_primary_key_fields_non_null(#ddl_v1{local_key = #key_v1{ast = LK},
                                           fields = Fields}) ->
    PKFieldNames = [N || #param_v1{name = [N]} <- LK],
    OnlyPKFields = [F || #riak_field_v1{name = N} = F <- Fields,
                         lists:member(N, PKFieldNames)],
    NonNullFields =
        [binary_to_list(F) || #riak_field_v1{name = F, optional = Null}
                                  <- OnlyPKFields, Null == true],
    case NonNullFields of
        [] ->
            ok;
        NonNullFields ->
            riak_ql_parser:return_error_flat("Primary key has 'null' fields (~s)",
                                                  [string:join(NonNullFields, ", ")])
    end.

%% @doc Assert that the partition key has at least one field.
assert_partition_key_length(#ddl_v1{partition_key = {key_v1, [_|_]}}) ->
    ok;
assert_partition_key_length(#ddl_v1{partition_key = {key_v1, Key}}) ->
    riak_ql_parser:return_error_flat("Primary key must have one or more fields ~p", [Key]).

%% @doc Verify primary key and local partition have the same elements
assert_primary_and_local_keys_match(#ddl_v1{partition_key = #key_v1{ast = Primary},
                                            local_key = #key_v1{ast = Local}}) ->
    PrimaryList = [query_field_name(F) || F <- Primary],
    LocalList = [query_field_name(F) || F <- lists:sublist(Local, length(PrimaryList))],
    case PrimaryList == LocalList of
        true ->
            ok;
        false ->
            riak_ql_parser:return_error_flat("Local key does not match primary key")
    end.

assert_unique_fields_in_pk(#ddl_v1{local_key = #key_v1{ast = LK}}) ->
    Fields = [N || #param_v1{name = [N]} <- LK],
    case length(Fields) == length(lists:usort(Fields)) of
        true ->
            ok;
        false ->
            riak_ql_parser:return_error_flat(
              "Primary key has duplicate fields (~s)",
              [string:join(
                 which_duplicate(
                   lists:sort(
                     [binary_to_list(F) || F <- Fields])),
                 ", ")])
    end.

%% Ensure that all fields in the primary key exist in the table definition.
assert_partition_key_fields_exist(#ddl_v1{ fields = Fields,
                                           partition_key =
                                               #key_v1{ ast = PK } }) ->
    MissingFields =
        [binary_to_list(name_of(F)) || F <- PK, not is_field(F, Fields)],
    case MissingFields of
        [] ->
            ok;
        _ ->
            riak_ql_parser:return_error_flat("Primary key fields do not exist (~s).",
                                                  [string:join(MissingFields, ", ")])
    end.

%% Check that the field name exists in the list of fields.
is_field(Field, Fields) ->
    (lists:keyfind(name_of(Field), 2, Fields) /= false).

%%
name_of(#param_v1{ name = [N] }) ->
    N;
name_of(#hash_fn_v1{ args = [#param_v1{ name = [N] }|_] }) ->
    N.

which_duplicate(FF) -> which_duplicate(FF, []).

which_duplicate([],              Acc) -> Acc;
which_duplicate([_],             Acc) -> Acc;
which_duplicate([A,A|_] = [_|T], Acc) -> which_duplicate(T, [A|Acc]);
which_duplicate([_|T],           Acc) -> which_duplicate(T, Acc).

%% Pull the name out of the appropriate record
query_field_name(#hash_fn_v1{args = Args}) ->
    Param = lists:keyfind(param_v1, 1, Args),
    query_field_name(Param);
query_field_name(#param_v1{name = Field}) ->
    Field.
