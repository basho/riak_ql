%% -------------------------------------------------------------------
%%
%% SELECT command tests for the Parser
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

-module(parser_select_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

select_sql_test() ->
    ?sql_comp_assert("select * from argle",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"argle">>}).

select_sql_with_semicolon_test() ->
    ?sql_comp_assert("select * from argle;",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"argle">>}).

select_sql_with_semicolons_in_quotes_test() ->
    ?sql_comp_assert("select * from \"table;name\" where ';' = asdf;",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"table;name">>,
                                 'WHERE'  = [{'=', <<"asdf">>, {binary, <<";">>}}]
                                }).

select_sql_semicolon_second_statement_test() ->
    ?sql_comp_fail("select * from asdf; select * from asdf").

select_sql_multiple_semicolon_test() ->
    ?sql_comp_fail("select * from asdf;;").

select_quoted_sql_test() ->
    ?sql_comp_assert("select * from \"argle\"",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"argle">>}).

select_quoted_keyword_sql_test() ->
    ?sql_comp_assert("select * from \"select\"",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"select">>}).

select_nested_quotes_sql_test() ->
    ?sql_comp_assert("select * from \"some \"\"quotes\"\" in me\"",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"some \"quotes\" in me">>}).

select_from_lists_sql_test() ->
    ?sql_comp_assert("select * from events, errors",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = {list, [<<"events">>, <<"errors">>]}
                                }).

select_fields_from_lists_sql_test() ->
    ?sql_comp_assert("select hip, hop, dont, stop from events",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                                                          {identifier, [<<"hip">>]},
                                                                          {identifier, [<<"hop">>]},
                                                                          {identifier, [<<"dont">>]},
                                                                          {identifier, [<<"stop">>]}
                                                                         ]},
                                 'FROM'   = <<"events">>
                                }).

select_quoted_spaces_sql_test() ->
    ?sql_comp_assert("select * from \"table with spaces\"",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"table with spaces">>}).




select_quoted_escape_sql_test() ->
    ?sql_comp_assert("select * from \"table with spaces\" where "
                     "\"co\"\"or\" = 'klingon''name' or "
                     "\"co\"\"or\" = '\"'",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"table with spaces">>,
                                 'WHERE' = [
                                            {or_,
                                             {'=', <<"co\"or">>, {binary, <<"\"">>}},
                                             {'=', <<"co\"or">>, {binary, <<"klingon'name">>}}
                                            }
                                           ]
                                }).
