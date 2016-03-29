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

-include_lib("eunit/include/eunit.hrl").

-include("riak_ql_ddl.hrl").

-compile([export_all]).

-define(sql_comp_assert(String, Type, Expected, CompilerCapability, QueryCapability),
        Toks = riak_ql_lexer:get_tokens(String),
        Result = riak_ql_parser:parse_TEST(Toks),
        {Type, Got} = riak_ql_parser:post_process_TEST(Result, CompilerCapability, QueryCapability),
        ?assertEqual(Expected, Got)).

-define(where_test(Uncanonical, Expected),
        Got = riak_ql_where_pipeline:canonicalise_where_TEST(Uncanonical),
        ?assertEqual(Expected, Got)).

-define(sql_comp_assert_match(String, Type, Expected, CompilerCapability, QueryCapability),
        Toks = riak_ql_lexer:get_tokens(String),
        Result = riak_ql_parser:parse_TEST(Toks),
        {Type, Got} = riak_ql_parser:post_process_TEST(Result, CompilerCapability, QueryCapability),
        %% only check the fields that the test has passed in
        Got2 = [{K, V} || {K, V} <- Got,
                          lists:keymember(K, 1, Expected)],
        SGot = lists:sort(Got2),
        SExpected = lists:sort(Expected),
        ?assertEqual(SExpected, SGot)).

-define(sql_comp_fail(QL_string),
        Toks = riak_ql_lexer:get_tokens(QL_string),
        Proplist = riak_ql_parser:parse_TEST(Toks),
        Got = try
                  riak_ql_parser:post_process_TEST(Proplist, {query_compiler, 2}, {query_coordinator, 1})
              catch throw:Err ->
                      Err
              end,
        ?assertMatch({error, _}, Got)).
