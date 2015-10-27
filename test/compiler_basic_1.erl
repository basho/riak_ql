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
               case riak_ql_ddl_compiler:mk_helper_m2(DDL) of
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

?passing_test(round_trip_test,
              "create table temperatures " ++
                  "(time timestamp not null, " ++
                  "user_id varchar not null, " ++
                  "primary key (time, user_id))",
              {12345, <<"beeees">>},
              [{timestamp, 12345}, {binary, <<"beeees">>}],
              [{timestamp, 12345}, {binary, <<"beeees">>}]).

?passing_short_test(integer_type_test,
                    "create table temperatures "
                    "(counter integer not null, "
                    "primary key (counter))",
              {12345}).

?passing_short_test(float_test,
                    "create table temperatures "
                    "(real_counter float not null, "
                    "primary key (real_counter))",
              {12345.6}).

?failing_test(round_trip_fail_test,
              "create table temperatures " ++
                  "(time timestamp not null, " ++
                  "user_id varchar not null, " ++
                  "primary key (time, user_id))",
              {<<"banjette">>, <<"beeees">>}).

?passing_test(no_partition_key_test,
              "create table temperatures " ++
                  "(time timestamp not null, " ++
                  "user_id varchar not null, " ++
                  "primary key (time, user_id))",
              {12345, <<"beeees">>},
              [{timestamp, 12345}, {binary, <<"beeees">>}],
              [{timestamp, 12345}, {binary, <<"beeees">>}]).

%%
%% roundtrip DDL tests
%%
?ddl_roundtrip_assert(round_trip_ddl_test,
              "create table temperatures " ++
                  "(time timestamp not null, " ++
                  "user_id varchar not null, " ++
                  "primary key (time, user_id))").

?ddl_roundtrip_assert(integer_type_ddl_test,
                    "create table temperatures "
                    "(counter integer not null, "
                    "primary key (counter))").

?ddl_roundtrip_assert(float_ddl_test,
                    "create table temperatures "
                    "(real_counter float not null, "
                    "primary key (real_counter))").

?ddl_roundtrip_assert(round_trip_ddl_2_test,
              "create table temperatures " ++
                  "(time timestamp not null, " ++
                  "user_id varchar not null, " ++
                  "primary key (time, user_id))").

?ddl_roundtrip_assert(no_partition_key_ddl_test,
              "create table temperatures " ++
                  "(time timestamp not null, " ++
                  "user_id varchar not null, " ++
                  "primary key (time, user_id))").
