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

%% @doc This module serves to generate time quanta on multi - (day, hour, minute,
%% second) boundaries. The quantum are based on an origin time of Jan 1, 1970 00:00:00 (Unix Epoch).
%% The function <em>quantum/3</em> takes a time in milliseconds to bucketize, a size of the quantum, and the
%% units of said quantum. For instance, the following call would create buckets for timestamps on 15
%% minute boundaries: <em>quantum(Time, 15, m)</em>. The quantum time is returned in milliseconds since the
%% Unix epoch.
%% the function <em>quanta/4</em> takes 2 times in milliseconds and size of the quantum
%% and the of units of said quantum and returns a list of quantum boundaries that span the time
-module(riak_ql_create_table_pipeline).

-export([
         make_create_table/2,
         make_create_table/3
        ]).

-include("riak_ql_ddl.hrl").

%%
%% API
%%
make_create_table(TableName, Contents) ->
    make_create_table(TableName, Contents, []).

make_create_table({identifier, Table}, Contents, Properties) ->
    DDL = ?DDL{table = Table,
               partition_key = find_partition_key(Contents),
               local_key     = find_local_key(Contents),
               fields        = find_fields(Contents)},
    {validate_ddl(DDL), validate_table_properties(Properties)}.

%%
%% Internal Functions
%%
find_partition_key({table_element_list, Elements}) ->
    find_partition_key(Elements);
find_partition_key([{partition_key, Key} | _Rest]) ->
    Key;
find_partition_key([_Head | Rest]) ->
    find_partition_key(Rest);
find_partition_key(_) ->
    none.

find_local_key({table_element_list, Elements}) ->
    find_local_key(Elements);
find_local_key([{local_key, Key} | _Rest]) ->
    Key;
find_local_key([_Head | Rest]) ->
    find_local_key(Rest);
find_local_key(_) ->
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

validate_table_properties(Properties) ->
    %% We let all k=v in: there's more substantial validation and
    %% enrichment happening in riak_kv_wm_utils:erlify_bucket_prop
    Properties.


%% DDL validation

validate_ddl(DDL) ->
    ok = assert_keys_present(DDL),
    ok = assert_unique_fields_in_pk(DDL),
    ok = assert_partition_key_length(DDL),
    ok = assert_primary_and_local_keys_match(DDL),
    ok = assert_partition_key_fields_exist(DDL),
    ok = assert_primary_key_fields_non_null(DDL),
    ok = assert_not_more_than_one_quantum(DDL),
    ok = assert_quantum_fn_args(DDL),
    ok = assert_quantum_is_last_in_partition_key(DDL),
    DDL.

%% @doc Ensure DDL has keys
assert_keys_present(?DDL{local_key = LK, partition_key = PK})
  when LK == none;
       PK == none ->
    riak_ql_parser:return_error_flat("Missing primary key");
assert_keys_present(_GoodDDL) ->
    ok.

%% @doc Ensure all fields appearing in PRIMARY KEY are not null.
assert_primary_key_fields_non_null(?DDL{local_key = #key_v1{ast = LK},
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
assert_partition_key_length(?DDL{partition_key = {key_v1, [_|_]}}) ->
    ok;
assert_partition_key_length(?DDL{partition_key = {key_v1, Key}}) ->
    riak_ql_parser:return_error_flat("Primary key must have one or more fields ~p", [Key]).

%% @doc Verify primary key and local partition have the same elements
assert_primary_and_local_keys_match(?DDL{partition_key = #key_v1{ast = Primary},
                                         local_key = #key_v1{ast = Local}}) ->
    PrimaryList = [query_field_name(F) || F <- Primary],
    LocalList = [query_field_name(F) || F <- lists:sublist(Local, length(PrimaryList))],
    case PrimaryList == LocalList of
        true ->
            ok;
        false ->
            riak_ql_parser:return_error_flat("Local key does not match primary key")
    end.

assert_unique_fields_in_pk(?DDL{local_key = #key_v1{ast = LK}}) ->
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
assert_partition_key_fields_exist(?DDL{ fields = Fields,
                                        partition_key = #key_v1{ ast = PK } }) ->
    MissingFields =
        [binary_to_list(name_of(F)) || F <- PK, not is_field(F, Fields)],
    case MissingFields of
        [] ->
            ok;
        _ ->
            riak_ql_parser:return_error_flat("Primary key includes non-existent fields (~s).",
                                             [string:join(MissingFields, ", ")])
    end.

assert_quantum_fn_args(#ddl_v1{ partition_key = #key_v1{ ast = PKAST } } = DDL) ->
    [assert_quantum_fn_args2(DDL, Args) || #hash_fn_v1{ mod = riak_ql_quanta, fn = quantum, args = Args } <- PKAST],
    ok.

%% The param argument is validated by assert_partition_key_fields_exist/1.
assert_quantum_fn_args2(DDL, [Param, Unit, Measure]) ->
    FieldName = name_of(Param),
    case riak_ql_ddl:get_field_type(DDL, FieldName) of
        {ok, timestamp} ->
            ok;
        {ok, InvalidType} ->
            riak_ql_parser:return_error_flat("Quantum field '~s' must be type of timestamp but was ~p.",
                                             [FieldName, InvalidType])
    end,
    case lists:member(Measure, [d,h,m,s]) of
        true ->
            ok;
        false ->
            riak_ql_parser:return_error_flat("Quantum time measure was ~p but must be d, h, m or s.",
                                             [Measure])
    end,
    case is_integer(Unit) andalso Unit >= 1 of
        true ->
            ok;
        false ->
            riak_ql_parser:return_error_flat("Quantum time unit must be a positive integer.", [])
    end.

assert_not_more_than_one_quantum(#ddl_v1{ partition_key = #key_v1{ ast = PKAST } }) ->
    QuantumFns =
        [Fn || #hash_fn_v1{ } = Fn <- PKAST],
    case length(QuantumFns) =< 1 of
        true ->
            ok;
        false ->
            riak_ql_parser:return_error_flat(
              "More than one quantum function in the partition key.", [])
    end.

assert_quantum_is_last_in_partition_key(#ddl_v1{ partition_key = #key_v1{ ast = PKAST } }) ->
    assert_quantum_is_last_in_partition_key2(PKAST).

%%
assert_quantum_is_last_in_partition_key2([]) ->
    ok;
assert_quantum_is_last_in_partition_key2([#hash_fn_v1{ }]) ->
    ok;
assert_quantum_is_last_in_partition_key2([#hash_fn_v1{ }|_]) ->
    riak_ql_parser:return_error_flat(
      "The quantum function must be the last element of the partition key.", []);
assert_quantum_is_last_in_partition_key2([_|Tail]) ->
    assert_quantum_is_last_in_partition_key2(Tail).

%% Check that the field name exists in the list of fields.
is_field(Field, Fields) ->
    (lists:keyfind(name_of(Field), 2, Fields) /= false).

%%
name_of(#param_v1{ name = [N] }) ->
    N;
name_of(#hash_fn_v1{ args = [#param_v1{ name = [N] }|_] }) ->
    N.

which_duplicate(FF) ->
    which_duplicate(FF, []).
which_duplicate([], Acc) ->
    Acc;
which_duplicate([_], Acc) ->
    Acc;
which_duplicate([A,A|_] = [_|T], Acc) ->
    which_duplicate(T, [A|Acc]);
which_duplicate([_|T], Acc) ->
    which_duplicate(T, Acc).

%% Pull the name out of the appropriate record
query_field_name(#hash_fn_v1{args = Args}) ->
    Param = lists:keyfind(param_v1, 1, Args),
    query_field_name(Param);
query_field_name(#param_v1{name = Field}) ->
    Field.
