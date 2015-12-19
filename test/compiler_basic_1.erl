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
                       GotPK = riak_ql_ddl:get_partition_key(DDL, Val),
                       GotLK = riak_ql_ddl:get_local_key(DDL, Val),
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
        " user_id varchar not null,"
        " user_di varchar not null,"
        " time timestamp not null,"
        " primary key ((user_id, user_di, quantum(time, 1, 'm')), user_id, user_di, time))").
-define(GOOD_DDL_INT,
        "create table temperatures ("
        " user_id varchar not null,"
        " counter sint64 not null, "
        " time timestamp not null,"
        " primary key ((user_id, counter, quantum(time, 1, 'm')), user_id, counter, time))").
-define(GOOD_DDL_DOUBLE,
        "create table temperatures ("
        " user_id varchar not null,"
        " bouble double not null, "
        " time timestamp not null,"
        " primary key ((user_id, bouble, quantum(time, 1, 'm')), user_id, bouble, time))").

?passing_test(round_trip_test,
              ?GOOD_DDL,
              {<<"beeees">>, <<"boooos">>, 12345},
              [{varchar, <<"beeees">>}, {varchar, <<"boooos">>}, {timestamp, 0}],
              [{varchar, <<"beeees">>}, {varchar, <<"boooos">>}, {timestamp, 12345}]).

?passing_short_test(sint64_type_test,
                    ?GOOD_DDL_INT,
                    { <<"boooos">>, 12345, 222222222}).

?passing_short_test(double_type_test,
                    ?GOOD_DDL_DOUBLE,
                    { <<"boooos">>, 12345.6, 222222222}).

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
