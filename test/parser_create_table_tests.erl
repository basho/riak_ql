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
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = case riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1}) of
              {ddl, D} -> D;
              _WC      -> wont_compile
          end,
    Expected = #ddl_v1{
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
                                            #param_v1{name = [<<"geohash">>]},
                                            #param_v1{name = [<<"user">>]},
                                            #hash_fn_v1{mod  = riak_ql_quanta,
                                                        fn   = quantum,
                                                        args = [
                                                                #param_v1{name = [<<"time">>]}, 15, m
                                                               ],
                                                        type = timestamp}
                                           ]},
                  local_key = #key_v1{
                                 ast = [
                                        #param_v1{name = [<<"geohash">>]},
                                        #param_v1{name = [<<"user">>]},
                                        #param_v1{name = [<<"time">>]}
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
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = case riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1}) of
              {ddl, D} -> D;
              WC       -> WC
          end,
    Expected = #ddl_v1{
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
                                #param_v1{name = [<<"user">>]},
                                #param_v1{name = [<<"geohash">>]},
                                #hash_fn_v1{mod  = riak_ql_quanta,
                                            fn   = quantum,
                                            args = [
                                                    #param_v1{name = [<<"time">>]}, 15, m
                                                   ],
                                            type = timestamp}
                               ]},
                  local_key =
                      #key_v1{
                         ast = [
                                #param_v1{name = [<<"user">>]},
                                #param_v1{name = [<<"geohash">>]},
                                #param_v1{name = [<<"time">>]}
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
    Toks = riak_ql_lexer:get_tokens(Table_def),
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = try
              riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1})
          catch throw:Err ->
                  Err
          end,
    ?assertEqual(
       {error, {0, riak_ql_parser, <<"Primary key fields do not exist (family).">>}},
       Got).

key_fields_must_exist_2_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time TIMESTAMP NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    Toks = riak_ql_lexer:get_tokens(Table_def),
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = try
              riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1})
          catch throw:Err ->
                  Err
          end,
    ?assertEqual(
       {error, {0, riak_ql_parser, <<"Primary key fields do not exist (family, series).">>}},
       Got).

key_fields_must_exist_3_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    Toks = riak_ql_lexer:get_tokens(Table_def),
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = try
              riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1})
          catch throw:Err ->
                  Err
          end,
    ?assertMatch(
       {error, {0, riak_ql_parser, <<"Primary key fields do not exist (time).">>}},
       Got).

create_table_white_space_test() ->
    Table_def =
        "CREATE               \tTABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "time TIMESTAMP NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    Toks = riak_ql_lexer:get_tokens(Table_def),
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1}),
    ?assertMatch(
       {ddl, #ddl_v1{}},
       Got).

primary_key_white_space_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "time TIMESTAMP NOT NULL, "
        "PRIMARY               \t  KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    Toks = riak_ql_lexer:get_tokens(Table_def),
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1}),
    ?assertMatch(
       {ddl, #ddl_v1{}},
       Got).

not_null_white_space_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "time TIMESTAMP NOT                NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    Toks = riak_ql_lexer:get_tokens(Table_def),
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1}),
    ?assertMatch(
       {ddl, #ddl_v1{}},
       Got).

short_key_1_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "time TIMESTAMP NOT                NULL, "
        "PRIMARY KEY "
        " ((quantum(time, 15, 's')), time))",
    Toks = riak_ql_lexer:get_tokens(Table_def),
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1}),
    ?assertMatch(
       {ddl, #ddl_v1{}},
       Got).

short_key_2_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((quantum(c, 15, 's')), c))",
    Toks = riak_ql_lexer:get_tokens(Table_def),
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1}),
    ?assertMatch(
       {ddl, #ddl_v1{}},
       Got).

no_quanta_in_primary_key_is_ok_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c SINT64 NOT NULL, "
        "PRIMARY KEY ((a,b), a,b,c))",
    Toks = riak_ql_lexer:get_tokens(Table_def),
    Proplist = riak_ql_parser:parse_TEST(Toks),
    Got = riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1}),
    ?assertMatch(
        {ddl, #ddl_v1{
                partition_key =
                    #key_v1{
                       ast = [
                              #param_v1{name = [<<"a">>]},
                              #param_v1{name = [<<"b">>]}
                             ]},
                local_key =
                    #key_v1{
                       ast = [
                              #param_v1{name = [<<"a">>]},
                              #param_v1{name = [<<"b">>]},
                              #param_v1{name = [<<"c">>]}
                             ]}}},
       Got).
