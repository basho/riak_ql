%% -------------------------------------------------------------------
%%
%% Table creation tests for the Parser
%%
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

-module(parser_create_table_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

create_no_key_sql_test() ->
    ?sql_comp_fail("create table temps ("
                   " time timestamp not null, "
                   " temp_k double)").

create_timeseries_sql_test() ->
    String =
        "CREATE TABLE GeoCheckin ("
        " geohash varchar not null,"
        " user varchar not null,"
        " time timestamp not null,"
        " weather varchar not null,"
        " temp varchar,"
        " PRIMARY KEY ((geohash, user, quantum(time, 15, 'm')), geohash, user, time))",
    Toks = riak_ql_lexer:get_tokens(String),
    Got = case riak_ql_parser:ql_parse(Toks) of
              {ddl, D, _Props} ->
                  D;
              _WC ->
                  wont_compile
          end,
    Expected = ?DDL{
                  table = <<"GeoCheckin">>,
                  fields = [
                            #riak_field_v1{
                               name = <<"geohash">>,
                               position = 1,
                               type = varchar,
                               optional = false},
                            #riak_field_v1{
                               name = <<"user">>,
                               position = 2,
                               type = varchar,
                               optional = false},
                            #riak_field_v1{
                               name = <<"time">>,
                               position = 3,
                               type = timestamp,
                               optional = false},
                            #riak_field_v1{
                               name = <<"weather">>,
                               position = 4,
                               type = varchar,
                               optional = false},
                            #riak_field_v1{
                               name = <<"temp">>,
                               position = 5,
                               type = varchar,
                               optional = true}
                           ],
                  partition_key = #key_v1{
                                     ast = [
                                            ?SQL_PARAM{name = [<<"geohash">>]},
                                            ?SQL_PARAM{name = [<<"user">>]},
                                            #hash_fn_v1{mod  = riak_ql_quanta,
                                                        fn   = quantum,
                                                        args = [
                                                                ?SQL_PARAM{name = [<<"time">>]}, 15, m
                                                               ],
                                                        type = timestamp}
                                           ]},
                  local_key = #key_v1{
                                 ast = [
                                        ?SQL_PARAM{name = [<<"geohash">>]},
                                        ?SQL_PARAM{name = [<<"user">>]},
                                        ?SQL_PARAM{name = [<<"time">>]}
                                       ]}
                 },
    ?assertEqual(Expected, Got).


create_all_types_sql_test() ->
    String =
        "CREATE TABLE GeoCheckin ("
        " user varchar not null,"
        " geohash varchar not null,"
        " time timestamp not null,"
        " isweather boolean not null,"
        " temp double not null,"
        " mysint64 sint64 not null,"
        " user2 varchar,"
        " time2 timestamp,"
        " isweather2 boolean,"
        " temp2 double,"
        " mysint642 sint64,"
        " PRIMARY KEY ((user, geohash, quantum(time, 15, 'm')),"
        " user, geohash, time))",
    Toks = riak_ql_lexer:get_tokens(String),
    Got = case riak_ql_parser:ql_parse(Toks) of
              {ddl, D, _Props} ->
                  D;
              WC ->
                  WC
          end,
    Expected = ?DDL{
                  table = <<"GeoCheckin">>,
                  fields = [
                            #riak_field_v1{
                               name = <<"user">>,
                               position = 1,
                               type = varchar,
                               optional = false},
                            #riak_field_v1{
                               name = <<"geohash">>,
                               position = 2,
                               type = varchar,
                               optional = false},
                            #riak_field_v1{
                               name = <<"time">>,
                               position = 3,
                               type = timestamp,
                               optional = false},
                            #riak_field_v1{
                               name = <<"isweather">>,
                               position = 4,
                               type = boolean,
                               optional = false},
                            #riak_field_v1{
                               name = <<"temp">>,
                               position = 5,
                               type = double,
                               optional = false},
                            #riak_field_v1{
                               name = <<"mysint64">>,
                               position = 6,
                               type = sint64,
                               optional = false},
                            #riak_field_v1{
                               name = <<"user2">>,
                               position = 7,
                               type = varchar,
                               optional = true},
                            #riak_field_v1{
                               name = <<"time2">>,
                               position = 8,
                               type = timestamp,
                               optional = true},
                            #riak_field_v1{
                               name = <<"isweather2">>,
                               position = 9,
                               type = boolean,
                               optional = true},
                            #riak_field_v1{
                               name = <<"temp2">>,
                               position = 10,
                               type = double,
                               optional = true},
                            #riak_field_v1{
                               name = <<"mysint642">>,
                               position = 11,
                               type = sint64,
                               optional = true}
                           ],
                  partition_key =
                      #key_v1{
                         ast = [
                                ?SQL_PARAM{name = [<<"user">>]},
                                ?SQL_PARAM{name = [<<"geohash">>]},
                                #hash_fn_v1{mod  = riak_ql_quanta,
                                            fn   = quantum,
                                            args = [
                                                    ?SQL_PARAM{name = [<<"time">>]}, 15, m
                                                   ],
                                            type = timestamp}
                               ]},
                  local_key =
                      #key_v1{
                         ast = [
                                ?SQL_PARAM{name = [<<"user">>]},
                                ?SQL_PARAM{name = [<<"geohash">>]},
                                ?SQL_PARAM{name = [<<"time">>]}
                               ]}
                 },
    ?assertEqual(Expected, Got).

create_with_timestamp_nullable_key_test() ->
    ?sql_comp_fail("CREATE TABLE GeoCheckin ("
                   " geohash varchar not null,"
                   " user varchar not null,"
                   " time timestamp,"
                   " weather varchar not null,"
                   " temp varchar,"
                   " PRIMARY KEY ((geohash, user, quantum(time, 15, 'm')), geohash, user, time))").

create_with_single_nullable_key_test() ->
    ?sql_comp_fail("CREATE TABLE GeoCheckin ("
                   " geohash varchar not null,"
                   " user varchar,"
                   " time timestamp not null,"
                   " weather varchar not null,"
                   " temp varchar,"
                   " PRIMARY KEY ((geohash, user, quantum(time, 15, 'm')), geohash, user, time))").

create_with_all_nullable_key_test() ->
    ?sql_comp_fail("CREATE TABLE GeoCheckin ("
                   " geohash varchar,"
                   " user varchar,"
                   " time timestamp,"
                   " weather varchar not null,"
                   " temp varchar,"
                   " PRIMARY KEY ((geohash, user, quantum(time, 15, 'm')), geohash, user, time))").

key_fields_must_exist_1_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time TIMESTAMP NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertEqual(
       {error, {0, riak_ql_parser, <<"Primary key includes non-existent fields (family).">>}},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).

key_fields_must_exist_2_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time TIMESTAMP NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertEqual(
       {error, {0, riak_ql_parser, <<"Primary key includes non-existent fields (family, series).">>}},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).

key_fields_must_exist_3_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
       {error, {0, riak_ql_parser, <<"Primary key includes non-existent fields (time).">>}},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).

create_table_white_space_test() ->
    Table_def =
        "CREATE               \tTABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "time TIMESTAMP NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
       {ddl, ?DDL{}, []},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).

primary_key_white_space_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "time TIMESTAMP NOT NULL, "
        "PRIMARY               \t  KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
       {ddl, ?DDL{}, []},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).

not_null_white_space_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "time TIMESTAMP NOT                NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
       {ddl, ?DDL{}, []},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).

short_key_1_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "time TIMESTAMP NOT                NULL, "
        "PRIMARY KEY "
        " ((quantum(time, 15, 's')), time))",
    ?assertMatch(
       {ddl, ?DDL{}, []},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).

short_key_2_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((quantum(c, 15, 's')), c))",
    ?assertMatch(
       {ddl, ?DDL{}, []},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).

no_quanta_in_primary_key_is_ok_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY ((a,b), a,b,c))",
    ?assertMatch(
       {ddl,
        ?DDL{
           partition_key =
               #key_v1{
                  ast = [
                         ?SQL_PARAM{name = [<<"a">>]},
                         ?SQL_PARAM{name = [<<"b">>]}
                        ]},
           local_key =
               #key_v1{
                  ast = [
                         ?SQL_PARAM{name = [<<"a">>]},
                         ?SQL_PARAM{name = [<<"b">>]},
                         ?SQL_PARAM{name = [<<"c">>]}
                        ]}},
        []},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).

multiple_quantum_functions_not_allowed_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((quantum(a, 15, 'm'),b,quantum(c, 15, 'm')), a,b,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"More than one quantum function in the partition key.">>}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

quantum_fn_field_must_exist_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(eh, 15, s)), a,b,eh))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"Primary key includes non-existent fields (eh).">>}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

quantum_fn_field_must_be_timestamp_test() ->
  Types = [boolean, double, sint64, varchar],
  [quantum_fn_field_must_be_timestamp_test_helper(T) || T <- Types].

quantum_fn_field_must_be_timestamp_test_helper(FieldType) ->
    FieldTypeString = atom_to_list(FieldType),
    FieldTypeBinary = atom_to_binary(FieldType, latin1),
    Table_def =
        "CREATE TABLE temperatures ("
        "a SINT64 NOT NULL, "
        "b SINT64 NOT NULL, "
        "c " ++ FieldTypeString ++ " NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 15, s)), a,b,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"Quantum field 'c' must be type of timestamp but was ", FieldTypeBinary/binary, ".">>}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

quantum_fn_second_arg_must_be_positive_integer_1_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, s, s)), a,b,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"Quantum time unit must be a positive integer.">>}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

quantum_fn_second_arg_must_be_positive_integer_2_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, -15, s)), a,b,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"Quantum time unit must be a positive integer.">>}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

quantum_fn_second_arg_must_be_positive_integer_3_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 15.5, s)), a,b,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"Quantum time unit must be a positive integer.">>}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

quantum_fn_last_arg_must_be_supported_quanta_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 15, 'y')), a,b,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"Quantum time measure was y but must be d, h, m or s.">>}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

quantum_fn_last_arg_no_quotes_required_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 15, s)), a,b,c))",
    {ok, {?DDL{ partition_key = #key_v1{ ast = PKAST } }, _}} =
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def)),
    ?assertEqual(
        [?SQL_PARAM{name = [<<"a">>]},
         ?SQL_PARAM{name = [<<"b">>]},
         #hash_fn_v1{mod = riak_ql_quanta,
                     fn = quantum,
                     args = [?SQL_PARAM{name = [<<"c">>]}, 15, s],
                     type = timestamp}],
        PKAST
      ).

quantum_must_be_last_in_the_partition_key_1_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((quantum(c, 15, 's'),a,b), c,a,b))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"The quantum function must be the last element of the partition key.">>}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

quantum_must_be_last_in_the_partition_key_2_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a TIMESTAMP NOT NULL, "
        "b SINT64 NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,quantum(c, 15, 's'),b), a,c,b))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"The quantum function must be the last element of the partition key.">>}},
        riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

create_with_1_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((quantum(c, 15, 's')), c))"
        " with ()",
    {ddl, _DDL, WithProps} =
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def)),
    ?assertEqual(
      [],
      WithProps).

create_with_2_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((quantum(c, 15, 's')), c))"
        " with (a ='2a', c= 3, d=0.5, e=true, f='')",
    {ddl, _DDL, WithProps} =
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def)),
    ?assertEqual(
      [{<<"a">>, <<"2a">>}, {<<"c">>, 3}, {<<"d">>, 0.5}, {<<"e">>, true}, {<<"f">>, <<>>}],
      WithProps).

partition_key_with_duplicate_fields_is_not_allowed_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,a,quantum(c, 15, s)), a,a,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"Primary key has duplicate fields (a)">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
    ).

partition_key_quantum_with_duplicate_fields_is_not_allowed_test() ->
    %% the quantum function uses the same field as another field in the
    %% partition key
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,c,quantum(c, 15, s)), a,c,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"Primary key has duplicate fields (c)">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
    ).

boolean_cannot_be_desc_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b BOOLEAN NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 15, s)), a,b DESC,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,
          <<"Elements in the local key marked descending (DESC) must be of type sint64 or varchar, but was boolean.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
    ).

float_cannot_be_desc_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b DOUBLE NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 15, s)), a,b DESC,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,
          <<"Elements in the local key marked descending (DESC) must be of type sint64 or varchar, but was double.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
    ).

desc_cannot_be_defined_on_the_partition_key_test() ->
    Table_def =
        "CREATE TABLE tab ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b DESC,quantum(c, 15, s)), a,b,c))",
    ?assertEqual(
        {error,{0,riak_ql_parser,
          <<"Order can only be used in the local key, 'b' set to descending">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
    ).

table_with_desc_keys_has_minimum_cap_v2_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((quantum(c, 15, 's')), c DESC))",
    {ddl, DDL, _} =
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def)),
    ?assertEqual(
        v2,
        DDL?DDL.minimum_capability
    ).

table_with_asc_keys_has_minimum_cap_v1_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((quantum(c, 15, 's')), c ASC))",
    {ddl, DDL, _} =
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def)),
    ?assertEqual(
        v1,
        DDL?DDL.minimum_capability
    ).

table_with_no_local_key_order_defined_is_v1_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((quantum(c, 15, 's')), c))",
    {ddl, DDL, _} =
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def)),
    ?assertEqual(
        v1,
        DDL?DDL.minimum_capability
    ).

%% definition for a primary key without brackets for the local key
missing_local_key_and_local_key_brackets_test() ->
    Table_def =
        "CREATE TABLE mytab1 ("
        "a varchar NOT NULL, "
        "ts timestamp NOT NULL, "
        "PRIMARY KEY(quantum(ts,30,'d')) );",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"No local key specified.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
    ).

missing_local_keys_test() ->
    Table_def =
        "CREATE TABLE mytab1 ("
        "a varchar NOT NULL, "
        "ts timestamp NOT NULL, "
        "PRIMARY KEY((quantum(ts,30,'d'))) );",
    ?assertEqual(
        {error,{0,riak_ql_parser,<<"No local key specified.">>}},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
    ).
