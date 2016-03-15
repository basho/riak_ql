%% -------------------------------------------------------------------
%%
%% riak_ql_sql_to_string: module that converts the output of the compiler
%%                        back to the text representation
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
-module(riak_ql_to_string).

-export([
         col_names_from_select/1
        ]).
%% --------------------------
%% local functions

%% Convert the selection in select clause to a list of strings, one
%% element for each column. White space in the original query is not reproduced.
-spec col_names_from_select(list(term())) -> [string()].
col_names_from_select(Select) ->
    [select_col_to_string(S) || S <- Select].

%% Convert one column to a flat string.
-spec select_col_to_string(any()) ->
                                  string().
%% these two happen only in sql
select_col_to_string(Bare) when is_binary(Bare) ->  %% bare column name in where expression
    binary_to_list(Bare);
select_col_to_string([Expr]) ->  %% a single where expression
    select_col_to_string(Expr);
%% these are common to ddl and sql:
select_col_to_string({identifier, [Name]}) ->
    binary_to_list(Name);
select_col_to_string({identifier, Name}) ->
    binary_to_list(Name);
select_col_to_string({integer, Value}) when is_integer(Value) ->
    integer_to_list(Value);
select_col_to_string({float, Value}) when is_float(Value) ->
    mochinum:digits(Value);
select_col_to_string({binary, Value}) when is_binary(Value) ->
    binary_to_list(<<"'", Value/binary, "'">>);
select_col_to_string({boolean, true}) ->
    "true";
select_col_to_string({boolean, false}) ->
    "false";
select_col_to_string({{window_agg_fn, FunName}, Args}) when is_atom(FunName) ->
    lists:flatten([
                   atom_to_list(FunName),
                   $(,
                   string:join([select_col_to_string(A) || A <- Args], ", "),
                   $)
                  ]);
select_col_to_string({expr, Expression}) ->
    select_col_to_string(Expression);
select_col_to_string({negate, Expression}) ->
    "-" ++ select_col_to_string(Expression);
select_col_to_string({Op, Arg1, Arg2}) when is_atom(Op) ->
    flat_format(
      "(~s~s~s)",
      [select_col_to_string(Arg1), op_to_string(Op), select_col_to_string(Arg2)]).

op_to_string(and_) -> "and";
op_to_string(or_) -> "or";
op_to_string(Op) ->
    atom_to_list(Op).

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

test_col_names(SQL, ColNames) ->
    {ok, Parsed} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
        SQL)),
    ?assertEqual(
        ColNames,
        col_names_from_select(proplists:get_value(fields, Parsed))
    ).

select_col_to_string_all_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select * from bendy")),
    ?assertEqual(
       ["*"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_colname_1_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select mindy from bendy")),
    ?assertEqual(
       ["mindy"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_colname_2_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select mindy, raymond from bendy")),
    ?assertEqual(
       ["mindy", "raymond"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_integer_literal_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select 1 from bendy")),
    ?assertEqual(
       ["1"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_boolean_true_literal_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select true from bendy")),
    ?assertEqual(
       ["true"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_boolean_false_literal_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select false from bendy")),
    ?assertEqual(
       ["false"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_double_literal_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select 7.2 from bendy")),
    ?assertEqual(
       ["7.2"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_varchar_literal_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select 'derp' from bendy")),
    ?assertEqual(
       ["'derp'"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_one_plus_one_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select 1+1 from bendy")),
    ?assertEqual(
       ["(1+1)"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_four_div_two_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select 4/2 from bendy")),
    ?assertEqual(
       ["(4/2)"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_four_times_ten_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select 4*10 from bendy")),
    ?assertEqual(
       ["(4*10)"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

select_col_to_string_avg_funcall_test() ->
    {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "select avg(mona) from bendy")),
    ?assertEqual(
       ["AVG(mona)"],
       col_names_from_select(proplists:get_value(fields, SQL))
      ).

% mixed aggregate and arithmetic are not in 1.1
%% select_col_to_string_avg_funcall_with_nested_maths_test() ->
%%     {ok, SQL} = riak_ql_parser:parse(riak_ql_lexer:get_tokens(
%%                                        "select avg(10+5) from bendy")),
%%     ?assertEqual(
%%        ["AVG((10+5))"],
%%        col_names_from_select(SQL)
%%       ).

select_col_to_string_negated_test() ->
    test_col_names("select - 1.0, - 1, -asdf, - asdf from dual",
                   ["-1.0",
                    "-1",
                    "-asdf",
                    "-asdf"]).

select_col_to_string_negated_parens_test() ->
    test_col_names("select -(1), -(asdf), -(3 + -4) from dual",
                   ["-1",
                    "-asdf",
                    "-(3+-4)"]).

-endif.
