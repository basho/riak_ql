%% -------------------------------------------------------------------
%%
%% Insert tests for the Parser
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
-module(parser_insert_tests).

-include_lib("eunit/include/eunit.hrl").

insert_boolean_true_test() ->
    Insert_sql =
        "INSERT INTO mytab (col) VALUES (true)",
    ?assertEqual(
        {insert,[{table,<<"mytab">>},
                                     {fields,[{identifier,[<<"col">>]}]},
                                     {values,[[{boolean,true}]]}]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Insert_sql))
    ).

insert_boolean_false_test() ->
    Insert_sql =
        "INSERT INTO mytab (col) VALUES (false)",
    ?assertEqual(
        {insert,[{table,<<"mytab">>},
                                     {fields,[{identifier,[<<"col">>]}]},
                                     {values,[[{boolean,false}]]}]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Insert_sql))
    ).

insert_double_test() ->
    Insert_sql =
        "INSERT INTO mytab (col) VALUES (3.5)",
    ?assertEqual(
        {insert,[{table,<<"mytab">>},
                                     {fields,[{identifier,[<<"col">>]}]},
                                     {values,[[{float,3.5}]]}]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Insert_sql))
    ).

insert_varchar_test() ->
    Insert_sql =
        "INSERT INTO mytab (col) VALUES ('qwerty')",
    ?assertEqual(
        {insert,[{table,<<"mytab">>},
                                     {fields,[{identifier,[<<"col">>]}]},
                                     {values,[[{binary,<<"qwerty">>}]]}]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Insert_sql))
    ).

insert_sint64_test() ->
    Insert_sql =
        "INSERT INTO mytab (col) VALUES (22)",
    ?assertEqual(
        {insert,[{table,<<"mytab">>},
                                     {fields,[{identifier,[<<"col">>]}]},
                                     {values,[[{integer,22}]]}]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Insert_sql))
    ).

insert_null_test() ->
    Insert_sql =
        "INSERT INTO mytab (col) VALUES (NULL)",
    ?assertEqual(
        {insert,[{table,<<"mytab">>},
                                    {fields,[{identifier,[<<"col">>]}]},
                                    {values,[[{null,<<"NULL">>}]]}]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Insert_sql))
    ).

insert_null_case_insensitive_test() ->
    Insert_sql =
        "INSERT INTO mytab (col) VALUES (NuLl)",
    ?assertEqual(
        {insert,[{table,<<"mytab">>},
                                    {fields,[{identifier,[<<"col">>]}]},
                                    {values,[[{null,<<"NuLl">>}]]}]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Insert_sql))
    ).

insert_identifier_test() ->
    Insert_sql =
        "INSERT INTO mytab (col) VALUES (john)",
    ?assertEqual(
        {insert,[{table,<<"mytab">>},
                                    {fields,[{identifier,[<<"col">>]}]},
                                    {values,[[{identifier,<<"john">>}]]}]},
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Insert_sql))
    ).

