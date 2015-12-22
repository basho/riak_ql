%% -------------------------------------------------------------------
%%
%% riak_ql_sql_to_txt: module that converts the output of the compiler
%%                     back to the text representation
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
-module(riak_ql_sql_to_txt).

-include("riak_ql_ddl.hrl").


-export([
         sql_to_txt/1,
         col_names_from_select/1
         ]).

%% TODO
%% needs to reverse out the compiled versions as well for John Daily/Andrei
sql_to_txt(#riak_sql_v1{'SELECT' = S,
                        'FROM'    = F,
                        'WHERE'   = W,
                        type     = sql}) ->
    SQL = [
           "SELECT",
           make_select_clause(select_to_col_names(S)),
           "FROM",
           make_from_clause(F),
           "WHERE",
           make_where_clause(W)
          ],
    string:join(SQL, " ");
sql_to_txt(#ddl_v1{} = DDL) ->
    gg:format("DDL is ~p~n", [DDL]),
    "brando".

make_select_clause(X) ->
    string:join(col_names_from_select(X), " ").

col_names_from_select(X) ->
    gg:format("X is ~p~n", [X]),
    "erko".

make_from_clause(_) ->
    "berko".

make_where_clause(_) ->
    "jerko".

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-define(sql_roundtrip_test(Name, SQL, ExpectedSQL),
        Name() ->
               Toks = riak_ql_lexer:get_tokens(SQL),
               {ok, Rec} = riak_ql_parser:parse(Toks),
               GotSQL = sql_to_txt(Rec),
               ?assertEqual(ExpectedSQL, GotSQL)).

?sql_roundtrip_test(basic_select_test, "select * from bendy", "SELECT * FROM bendy").


%% "select value from response_times where time > 1388534400",

%% "select value from response_times where time > 1388534400s",

%% "select * from events where time = 1400497861762723 "++

%% "select * from events where state = 'NY'",

%% "select * from events where customer_id = 23 and type = 'click10'",

%% "select * from response_times where value > 500",

%% "select * from response_times where value >= 500",

%% "select * from response_times where value <= 500",

%% "select * from events where signed_in = false",

%% "select * from events where signed_in = -3",

%% "select * from events where user = 'user 1'",

%% "select weather from GeoCheckin where time > 2000 and time < 8000 and user = 'user_1'",

%% "select weather from GeoCheckin where user = 'user_1' and location = 'San Francisco'",

%% "select * FROM tsall2 WHERE d3 = 1.0 OR d3 = 2.0 AND vc1nn != '2' AND vc2nn != '3' AND 0 < ts1nn  AND ts1nn < 1",

%% "select * FROM tsall2 WHERE d3 = 1.0 OR d3 = 2.0 AND vc1nn != '2' AND vc2nn != '3' AND 0 < ts1nn  AND ts1nn < 1 OR d3 = 3.0 OR d3 = 4.0",

%% "select * from events where user = 'user 1'",

%% "select weather from GeoCheckin where time > 2000 and time < 8000 and user = 'user_1'",

%% "select weather from GeoCheckin where (time > 2000 and time < 8000) and user = 'user_1'",

%% "select weather from GeoCheckin where user = 'user_1' and (time > 2000 and time < 8000)",

%% "select weather from GeoCheckin where user = 'user_1' and (time > 2000 and (time < 8000))",

%% "select * from \"table with spaces\"",

%% "select * from \"table with spaces\" where \"color spaces\" = 'someone had painted it blue'",

%% "select * from \"table with spaces\" where \"co\"\"or\" = 'klingon''name' or \"co\"\"or\" = '\"'"

%% "select temperature + 1 from details",

%% "select avg(temp) from details",

%% "select mean(temp) from details",

%% "select avg(temp), sum(counts) from details",

%% "select count(*) from details",

%% "select aVg(temp) from details",

%% "select avg(temp), sum(counts), count(counts), min(counts) "max(counts), stdev(counts) from details",

%% "select aVg(temperature) + 1 - 2 * 3 / 4 from details",

%% "select aVg(temperature) + count(temperature) from details",

%% TODO
%% this one wont work yet
%% %% "select aVg(temperature + 1) + count(temperature / distance) from details",

-endif.
