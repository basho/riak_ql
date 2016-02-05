%% -------------------------------------------------------------------
%%
%% query_ddl: a test suite for queries against DDLs
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
-module(query_ddl).

-include_lib("eunit/include/eunit.hrl").

-define(VALID,   true).
-define(INVALID, false).

%%
%% if you don't use indirection here
%% (ie you tried to have a single macro and pass in true/false as an arguement)
%% the bloody compiler detects that some code branches can't run and gives an error
%%
%% which is why we have a valid and an invalid test macro
%%
-define(valid_query_test(Name, CreateTable, SQLQuery),
        Name() -> run_test(Name, CreateTable, SQLQuery, ?VALID)).

-define(invalid_query_test(Name, CreateTable, SQLQuery),
        Name() -> run_test(Name, CreateTable, SQLQuery, ?INVALID)).

run_test(Name, CreateTable, SQLQuery, IsValid) ->
    Lexed = riak_ql_lexer:get_tokens(CreateTable),
    {ok, DDL} = riak_ql_parser:parse(Lexed),
    case riak_ql_ddl_compiler:compile_and_load_from_tmp(DDL) of
        {module, Module} ->
            Lexed2 = riak_ql_lexer:get_tokens(SQLQuery),
            Qry = riak_ql_parser:parse(Lexed2),
            case Qry of
                {ok, Q} -> case riak_ql_ddl:is_query_valid(Module, DDL, Q) of
                               true ->
                                   case IsValid of
                                       true ->
                                           ?assert(true);
                                       false ->
                                           ?debugFmt("Query in ~p should not be valid", [Name]),
                                           ?assert(false)
                                   end;
                               {false, E} ->
                                   case IsValid of
                                       true ->
                                           ?debugFmt("Test ~p failed with query syntax error of ~p~n",
                                                     [Name, E]),
                                           ?assert(false);
                                       false ->
                                           ?assert(true)
                                   end
                           end;
                Err     -> ?debugFmt("Test ~p failed with error ~p~n", [Name, Err]),
                           ?assert(false)
            end;
        _Other ->
            ?debugFmt("~n~p compilation failed:~n~p", [Name, _Other]),
            ?assert(false)
    end.


-define(STANDARDTABLE,
        "CREATE TABLE GeoCheckin "
        ++ "(geohash varchar not null, "
        ++ "user varchar not null, "
        ++ "time timestamp not null, "
        ++ "mytimestamp timestamp not null, "
        ++ "myboolean boolean not null, "
        ++ "mydouble double not null, "
        ++ "mysint64 sint64 not null, "
        ++ "myvarchar varchar not null, "
        ++ "PRIMARY KEY ((geohash, user, quantum(time, 15, 'm')), geohash, user, time))").

-define(SQL, "SELECT * FROM GeoCheckin WHERE " ++
            "geohash = 'erk' and user = 'berk' and time > 1 and time < 1000 and ").

%% Timestamps

?valid_query_test(timestamp_1_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mytimestamp = 3").

?valid_query_test(timestamp_2_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mytimestamp != 3").

?valid_query_test(timestamp_3_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mytimestamp < 3").

?valid_query_test(timestamp_4_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mytimestamp <= 3").

?valid_query_test(timestamp_5_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mytimestamp > 3").

?valid_query_test(timestamp_6_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mytimestamp >= 3").

%% booleans

?valid_query_test(boolean_1_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "myboolean = true").

?valid_query_test(boolean_1a_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "myboolean = True").

?valid_query_test(boolean_1b_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "myboolean = false").

?valid_query_test(boolean_1c_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "myboolean = False").

?invalid_query_test(boolean_1d_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myboolean = 'yardle'").

?valid_query_test(boolean_2_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "myboolean != true").

?invalid_query_test(boolean_3_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myboolean < 3.4").

?invalid_query_test(boolean_3a_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myboolean < true").

?invalid_query_test(boolean_4_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myboolean <= 3.4").

?invalid_query_test(boolean_4a_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myboolean <= false").

?invalid_query_test(boolean_5_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myboolean > 3.4").

?invalid_query_test(boolean_5a_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myboolean > true").

?invalid_query_test(boolean_6_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myboolean >= 3.4").

?invalid_query_test(boolean_6a_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myboolean >= fALse").

%% Doubles

?valid_query_test(double_1_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mydouble = 3.4").

?valid_query_test(double_2_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mydouble != 3.4").

?valid_query_test(double_3_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mydouble < 3.4").

?valid_query_test(double_4_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mydouble <= 3.4").

?valid_query_test(double_5_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mydouble > 3.4").

?valid_query_test(double_6_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mydouble >= 3.4").

%% sint64s

?valid_query_test(sint64_1_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mysint64 = 3").

?valid_query_test(sint64_2_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mysint64 != 3").

?valid_query_test(sint64_3_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mysint64 < 3").

?valid_query_test(sint64_4_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mysint64 <= 3").

?valid_query_test(sint64_5_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mysint64 > 3").

?valid_query_test(sint64_6_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "mysint64 >= 3").

%% varchars

?valid_query_test(varchar_1_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "myvarchar = 'eert'").

?valid_query_test(varchar_2_test,
                  ?STANDARDTABLE,
                  ?SQL ++ "myvarchar != 'wertetr'").

?invalid_query_test(varchar_3_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myvarchar < 3.4").

?invalid_query_test(varchar_4_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myvarchar <= 3.4").

?invalid_query_test(varchar_5_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myvarchar > 3.4").

?invalid_query_test(varchar_6_test,
                    ?STANDARDTABLE,
                    ?SQL ++ "myvarchar >= 3.4").
