%%-------------------------------------------------------------------
%%
%% SHOW CREATE TABLE SQL command
%%
%% These are retrieved from riak_core_bucket:get_bucket/1
%%
%% Copyright (C) 2016 Basho Technologies, Inc. All rights reserved
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
%%-------------------------------------------------------------------

-module(riak_ql_show_create_table).

-export([show_create_table/2]).

-include("riak_ql_ddl.hrl").

%%
-spec show_create_table(?DDL{}, [tuple()]) -> {ok, {ColNames::[binary()],
                                               ColTypes::[riak_ql_ddl:external_field_type()],
                                               Rows::[[any()]]}}.

show_create_table(DDL, Props) ->
    FilteredProps = filter_bucket_properties(Props),
    ColumnNames = [<<"SQL">>],
    ColumnTypes = [varchar],
    SQL = riak_ql_to_string:ddl_rec_to_sql_multiline(DDL, FilteredProps),
    Rows =
        [[SQL]],
    {ok, {ColumnNames, ColumnTypes, Rows}}.

%% Skip any internal bucket properties
filter_bucket_properties(Props) ->
    Filtered = lists:filter(fun({basic_quorum, _}) -> false;
                               ({big_vclock, _}) -> false;
                               ({chash_keyfun, _}) -> false;
                               ({claimant, _}) -> false;
                               ({ddl, _}) -> false;
                               ({ddl_compiler_version, _}) -> false;
                               ({linkfun, _}) -> false;
                               ({name, _}) -> false;
                               ({old_vclock, _}) -> false;
                               ({precommit, _}) -> false;
                               ({small_vclock, _}) -> false;
                               ({write_once, _}) -> false;
                               ({young_vclock, _}) -> false;
                               (_) -> true end, Props),
    lists:sort(fun({A,_},{B,_}) -> A =< B end, Filtered).


%%-------------------------------------------------------------------
%% Unit tests
%%-------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

matching_sql_test() ->
    SQL = "CREATE TABLE tab ("
    "a VARCHAR   NOT NULL, "
    "b VARCHAR   NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "PRIMARY KEY ((a, b, quantum(c, 15, 'm')), a, b, c)) "
    "WITH (a = 1, b = 'bee', c = false, d = 3.1415)",
    Props = [{<<"a">>,1},{<<"b">>,<<"bee">>},{<<"c">>,false},{<<"d">>,3.1415}],
    {ddl, DDL, Props} =
        riak_ql_parser:ql_parse(
          riak_ql_lexer:get_tokens(SQL)),
    {ok, Result} = show_create_table(DDL, Props),
    {Cols, _, [[Row]]} = Result,
    ?assertEqual(
        [<<"SQL">>],
        Cols),
    ?assertEqual(
        lowercase(SQL),
        lowercase(Row)
    ).

matching_sql_no_props_test() ->
    SQL = "CREATE TABLE tab ("
    "a VARCHAR   NOT NULL, "
    "b VARCHAR   NOT NULL, "
    "c TIMESTAMP NOT NULL, "
    "PRIMARY KEY ((a, b, quantum(c, 15, 'm')), a, b, c))",
    {ddl, DDL, []} =
        riak_ql_parser:ql_parse(
            riak_ql_lexer:get_tokens(SQL)),
    {ok, Result} = show_create_table(DDL, []),
    {Cols, _, [[Row]]} = Result,
    ?assertEqual(
        [<<"SQL">>],
        Cols),
    ?assertEqual(
        lowercase(SQL),
        lowercase(Row)
    ).

%% Remove the extra whitespace and lowercase everything for a safe comparison
lowercase(S) when is_list(S) ->
    SingleSpace = re:replace(S, "\\s+", " ", [global,{return,list}]),
    string:to_lower(string:join(string:tokens(SingleSpace, " "), " "));
lowercase(S) when is_binary(S) ->
    lowercase(binary_to_list(S)).

-endif.
