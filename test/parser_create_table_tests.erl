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

create_wrong_field_order_sql_test() ->
    ?sql_comp_fail("create table consufed ("
                   " a varchar not null, b varchar not null, c timestamp not null,"
                   " PRIMARY KEY ((a, c, quantum(b, 15, 'm')), a, c, b))").


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
              {ddl, D} -> D;
              _WC     -> wont_compile
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
    Got = case riak_ql_parser:ql_parse(Toks) of
              {ddl, D} -> D;
              WC     -> WC
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
    ?assertEqual(
       {error, {0, riak_ql_parser, <<"Primary key fields do not exist (family).">>}},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).

key_fields_must_exist_2_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time TIMESTAMP NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertEqual(
       {error, {0, riak_ql_parser, <<"Primary key fields do not exist (family, series).">>}},
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
       {error, {0, riak_ql_parser, <<"Primary key fields do not exist (time).">>}},
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
       {ddl, #ddl_v1{}},
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
       {ddl, #ddl_v1{}},
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
       {ddl, #ddl_v1{}},
       riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def))
      ).
