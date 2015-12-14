%% -------------------------------------------------------------------
%%
%% a basic test suite for the compiler
%%
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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
-module(compiler_basic_1).

-include_lib("eunit/include/eunit.hrl").

%% this is a basic test of timeseries that writes a single element to the back end
%% and checks it is correct

-define(VALID,   true).
-define(INVALID, false).

-compile(export_all).

%%
%% this test calls into the PRIVATE interface
%% mk_helper_m2/1
%%
-define(passing_test(Name, Query, Val, ExpectedPK, ExpectedLK),
        Name() ->
               Lexed = riak_ql_lexer:get_tokens(Query),
               {ok, DDL} = riak_ql_parser:parse(Lexed),
               case riak_ql_ddl_compiler:mk_helper_m2(DDL, "/tmp") of
                   {module, Module}  ->
                       Result = Module:validate_obj(Val),
                       GotPK = Module:get_partition_key(Val),
                       GotLK = Module:get_local_key(Val),
                       Expected = {?VALID, ExpectedPK, ExpectedLK},
                       Got = {Result, GotPK, GotLK},
                       ?assertEqual(Expected, Got);
                   _Other ->
                       ?debugFmt("~n~p compilation failed:~n~p", [Name, _Other]),
                       ?assert(false)
               end).

-define(passing_short_test(Name, Query, Val),
        Name() ->
               Lexed = riak_ql_lexer:get_tokens(Query),
               {ok, DDL} = riak_ql_parser:parse(Lexed),
               case riak_ql_ddl_compiler:mk_helper_m2(DDL) of
                   {module, Module}  ->
                       Result = Module:validate_obj(Val),
                       ?assertEqual(?VALID, Result);
                   _Other ->
                       ?debugFmt("~n~p compilation failed:~n~p", [Name, _Other]),
                       ?assert(false)
               end).


%%
%% this test calls into the PRIVATE interface
%% mk_helper_m2/1
%%
-define(failing_test(Name, Query, Val),
        Name() ->
               Lexed = riak_ql_lexer:get_tokens(Query),
               {ok, DDL} = riak_ql_parser:parse(Lexed),
               case riak_ql_ddl_compiler:mk_helper_m2(DDL) of
                   {module, Module}  ->
                       Result = Module:validate_obj(Val),
                       ?assertEqual(?INVALID, Result);
                   _Other ->
                       ?assertEqual(?INVALID, false) % didn't compile
               end).


%%
%% this test calls in the PUBLIC interface
%% make_helper_mod/1
%%
-define(not_valid_test(Name, Query),
        Name() ->
               Lexed = riak_ql_lexer:get_tokens(Query),
               {ok, DDL} = riak_ql_parser:parse(Lexed),
               case riak_ql_ddl_compiler:make_helper_mod(DDL) of
                   {error, _} ->
                       ?assertEqual(?VALID, true);
                   Other ->
                       ?assertEqual(?VALID, Other) % didn't compile
               end).


%%
%% this test tests that the DDL returned by the helper fun is
%% the same as the DDL that the helper fun was compiled from
%%
-define(ddl_roundtrip_assert(Name, Query),
        Name() ->
               Lexed = riak_ql_lexer:get_tokens(Query),
               {ok, DDL} = riak_ql_parser:parse(Lexed),
               %% ?debugFmt("in ~p~n- DDL is:~n -~p~n", [Name, DDL]),
               {module, Module} = riak_ql_ddl_compiler:mk_helper_m2(DDL),
               Got = Module:get_ddl(),
               %% ?debugFmt("in ~p~n- Got is:~n -~p~n", [Name, Got]),
               ?assertEqual(DDL, Got)).

%%
%% round trip passing tests
%%

-define(GOOD_DDL,
        "create table temperatures ("
        " time timestamp not null,"
        " user_id varchar not null,"
        " user_di varchar not null,"
        " primary key ((user_id, user_di, quantum(time, 1, 'm')), user_id, user_di, time))").
-define(GOOD_DDL_INT,
        "create table temperatures ("
        " counter sint64 not null, "
        " time timestamp not null,"
        " user_id varchar not null,"
        " primary key ((user_id, counter, quantum(time, 1, 'm')), user_id, counter, time))").
-define(GOOD_DDL_DOUBLE,
        "create table temperatures ("
        " bouble double not null, "
        " time timestamp not null,"
        " user_id varchar not null,"
        " primary key ((user_id, bouble, quantum(time, 1, 'm')), user_id, bouble, time))").

% round_trip_test() ->
%     {ok, DDL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(?GOOD_DDL)),
%     {module, Module} = riak_ql_ddl_compiler:mk_helper_m2(DDL),
%     Result = Module:validate_obj([12345, <<"beeees">>, <<"boooos">>]),
%     Expected = {?VALID, ExpectedPK, ExpectedLK},
%     Got = {Result, GotPK, GotLK},
%     ?assertEqual(
%         ExpectedLK,
%         Module:get_local_key(Val)
%     ),
%     ?assertEqual(
%         ExpectedPK,
%         Module:get_partition_key(Val)
%     ).

?passing_short_test(sint64_type_test,
                    ?GOOD_DDL_INT,
                    {12345, 222222222, <<"boooos">>}).

?passing_short_test(double_type_test,
                    ?GOOD_DDL_DOUBLE,
                    {12345.6, 222222222, <<"boooos">>}).

?failing_test(round_trip_fail_test,
              ?GOOD_DDL,
              {<<"banjette">>, <<"beeees">>}).

%%
%% roundtrip DDL tests
%%
?ddl_roundtrip_assert(round_trip_ddl_test,
                      ?GOOD_DDL).

?ddl_roundtrip_assert(sint64_type_ddl_test,
                      ?GOOD_DDL_INT).

?ddl_roundtrip_assert(double_ddl_test,
                      ?GOOD_DDL_DOUBLE).

helper_module_get_local_key_three_elems_1_test() ->
    Table_def =
        "CREATE table temperatures ("
        "counter SINT64 NOT NULL, "
        "time TIMESTAMP NOT NULL,"
        "userid VARCHAR NOT NULL,"
        "PRIMARY KEY ((userid, counter, QUANTUM(time, 1, 'm')), userid, counter, time))",
    {ok, DDL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def)),
    {module, Mod} = riak_ql_ddl_compiler:mk_helper_m2(DDL, "/tmp"),
    ?assertEqual(
        {userid, counter, time},
        Mod:get_local_key({counter, time, userid})
    ).

helper_module_get_local_key_three_elems_2_test() ->
    Table_def =
        "CREATE table helper_module_get_local_key_three_elems_2_test ("
        "counter SINT64 NOT NULL, "
        "userid VARCHAR NOT NULL,"
        "time TIMESTAMP NOT NULL,"
        "PRIMARY KEY ((counter, userid, QUANTUM(time, 1, 'm')), counter, userid, time))",
    {ok, DDL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def)),
    {module, Mod} = riak_ql_ddl_compiler:mk_helper_m2(DDL),
    ?assertEqual(
        {counter, userid, time},
        Mod:get_local_key({counter, userid, time})
    ).

helper_module_get_local_key_five_elems_1_test() ->
    Table_def =
        "CREATE table helper_module_get_local_key_five_elems_1_test ("
        "a SINT64 NOT NULL, "
        "b VARCHAR NOT NULL,"
        "c TIMESTAMP NOT NULL,"
        "d DOUBLE NOT NULL, "
        "e BOOLEAN NOT NULL, "
        "PRIMARY KEY ((b, e, QUANTUM(c, 1, 'm')), b, e, c))",
    {ok, DDL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def)),
    {module, Mod} = riak_ql_ddl_compiler:mk_helper_m2(DDL),
    ?assertEqual(
        {b, e, c},
        Mod:get_local_key({a,b,c,d,e})
    ).

helper_module_get_partition_key_five_elems_1_test() ->
    Table_def =
        "CREATE table helper_module_get_local_key_five_elems_1_test ("
        "a SINT64 NOT NULL, "
        "b VARCHAR NOT NULL,"
        "c TIMESTAMP NOT NULL,"
        "d DOUBLE NOT NULL, "
        "e BOOLEAN NOT NULL, "
        "PRIMARY KEY ((b, e, QUANTUM(c, 1, 'm')), b, e, c))",
    {ok, DDL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def)),
    {module, Mod} = riak_ql_ddl_compiler:mk_helper_m2(DDL),
    ?assertEqual(
        {b, e, riak_ql_quanta:quantum(10, 1, m)},
        Mod:get_partition_key({a,b,10,d,e})
    ).
