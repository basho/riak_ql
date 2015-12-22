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


% -export([sql_to_txt/1]).
-export([col_names_from_select/1]).

%% TODO
%% needs to reverse out the compiled versions as well for John Daily/Andrei
% sql_to_txt(#riak_sql_v1{'SELECT' = S,
%                         'FROM'    = F,
%                         'WHERE'   = W,
%                         type     = sql}) ->
%     SQL = [
%         "SELECT",
%         make_select_clause(select_to_col_names(S)),
%         "FROM",
%         make_from_clause(F),
%         "WHERE",
%         make_where_clause(W)
%     ],
%     string:join(SQL, " ");
% sql_to_txt(#ddl_v1{} = DDL) ->
%     gg:format("DDL is ~p~n", [DDL]),
%     "brando".


% make_select_clause(X) ->
%     string:join(col_names_from_select(X), " ").


%% Convert the selection in a #riak_sql_v1 statement to a list of strings, one
%% element for each column. White space in the original query is not reproduced.
-spec col_names_from_select(#riak_sql_v1{}) -> [string()].
col_names_from_select(#riak_sql_v1{ 'SELECT' = Select }) ->
    [select_col_to_string(S) || S <- Select].

%% Convert one column to a flat string.
-spec select_col_to_string(any()) -> 
        string().
select_col_to_string({identifier, [Name]}) ->
    binary_to_list(Name);
select_col_to_string({identifier, Name}) ->
    binary_to_list(Name);
select_col_to_string({integer, Value}) when is_integer(Value) ->
    integer_to_list(Value);
select_col_to_string({float, Value}) when is_float(Value) ->
    float_to_list(Value);
select_col_to_string({binary, Value}) when is_binary(Value) ->
    binary_to_list(<<"'", Value/binary, "'">>);
select_col_to_string({boolean, true}) ->
    "true";
select_col_to_string({boolean, false}) ->
    "false";
select_col_to_string({funcall, {FunName, Args}}) when is_atom(FunName) ->
    lists:flatten([
        atom_to_list(FunName),
        $(,
        string:join([select_col_to_string(A) || A <- Args], ", "),
        $)
    ]);
select_col_to_string({expr, Expression}) ->
    select_col_to_string(Expression);
select_col_to_string({Op, Arg1, Arg2}) when is_atom(Op) ->
    lists:flatten(
        [select_col_to_string(Arg1), atom_to_list(Op), select_col_to_string(Arg2)]).                                            
    

% make_from_clause(_) ->
%     "berko".

% make_where_clause(_) ->
%     "jerko".

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

select_col_to_string_all_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select * from bendy")),
    ?assertEqual(
        ["*"],
        col_names_from_select(SQL)
    ).

select_col_to_string_colname_1_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select mindy from bendy")),
    ?assertEqual(
        ["mindy"],
        col_names_from_select(SQL)
    ).

select_col_to_string_colname_2_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select mindy, raymond from bendy")),
    ?assertEqual(
        ["mindy", "raymond"],
        col_names_from_select(SQL)
    ).

select_col_to_string_integer_literal_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select 1 from bendy")),
    ?assertEqual(
        ["1"],
        col_names_from_select(SQL)
    ).

select_col_to_string_boolean_true_literal_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select true from bendy")),
    ?assertEqual(
        ["true"],
        col_names_from_select(SQL)
    ).

select_col_to_string_boolean_false_literal_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select false from bendy")),
    ?assertEqual(
        ["false"],
        col_names_from_select(SQL)
    ).

select_col_to_string_double_literal_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select 7.2 from bendy")),
    ?assertEqual(
        ["7.20000000000000017764e+00"], % weird float formatting!
        col_names_from_select(SQL)
    ).

select_col_to_string_varchar_literal_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select 'derp' from bendy")),
    ?assertEqual(
        ["'derp'"],
        col_names_from_select(SQL)
    ).

select_col_to_string_one_plus_one_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select 1+1 from bendy")),
    ?assertEqual(
        ["1+1"],
        col_names_from_select(SQL)
    ).

select_col_to_string_four_div_two_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select 4/2 from bendy")),
    ?assertEqual(
        ["4/2"],
        col_names_from_select(SQL)
    ).

select_col_to_string_four_times_ten_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select 4*10 from bendy")),
    ?assertEqual(
        ["4*10"],
        col_names_from_select(SQL)
    ).

select_col_to_string_avg_funcall_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select avg(mona) from bendy")),
    ?assertEqual(
        ["AVG(mona)"],
        col_names_from_select(SQL)
    ).

select_col_to_string_avg_funcall_with_nested_maths_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        "select avg(10+5) from bendy")),
    ?assertEqual(
        ["AVG(10+5)"],
        col_names_from_select(SQL)
    ).

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