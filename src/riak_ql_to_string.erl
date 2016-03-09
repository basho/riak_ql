%% -------------------------------------------------------------------
%%
%% riak_ql_to_string: module that converts the output of the compiler
%%                    back to the text representation
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

-export([sql_to_string/1,
         col_names_from_select/1]).

-include("riak_ql_ddl.hrl").

-spec sql_to_string({select|describe, proplists:proplist()} | {ddl, ?DDL{}}) ->
                           string().
sql_to_string({select, Parts}) ->
    Fields = proplists:get_value(fields, Parts),
    Tables = proplists:get_value(tables, Parts),
    Where  = proplists:get_value(where,  Parts),
    SQL = [
           "SELECT",
           make_select_clause(Fields),
           "FROM",
           make_from_clause(Tables),
           "WHERE",
           make_where_clause(Where)
           %% don't forget to add 'ORDER BY' and 'LIMIT' when/if appropriate
          ],
    string:join(SQL, " ");

sql_to_string({ddl, ?DDL{table         = T,
                         fields        = FF,
                         partition_key = PK,
                         local_key     = LK,
                         properties    = PP}}) ->
    flat_format(
      "CREATE TABLE ~s (~s, PRIMARY KEY ((~s), ~s))~s",
      [T, make_fields(FF), make_key(PK), make_key(LK), make_props(PP)]).


%% Convert the selection in select clause to a list of strings, one
%% element for each column. White space in the original query is not reproduced.
-spec col_names_from_select(list(term())) -> [string()].
col_names_from_select(Select) ->
    [select_col_to_string(S) || S <- Select].

%% --------------------------
%% local functions

make_select_clause(FF) ->
    string:join([select_col_to_string(F) || F <- FF], ", ").

 
make_from_clause(X) when is_binary(X) ->
    binary_to_list(X);
make_from_clause({list, XX}) ->
    string:join(
      [binary_to_list(X) || X <- XX], ", ");
make_from_clause({regex, XX}) ->
    XX.

make_where_clause(XX) ->
    select_col_to_string(XX).


make_fields(FF) ->
    string:join(
      [make_field(F) || F <- FF], ", ").

make_field(#riak_field_v1{name = N, type = T, optional = Optional}) ->
    flat_format("~s ~s~s", [N, T, not_null_or_not(Optional)]).

not_null_or_not(true)  -> "";
not_null_or_not(false) -> " not null".

make_key(#key_v1{ast = FF}) ->
    string:join(
      [make_key_element(F) || F <- FF], ", ").

make_key_element(#param_v1{name = [F]}) ->
    binary_to_list(F);
make_key_element(#hash_fn_v1{mod = riak_ql_quanta, fn = quantum,
                             args = [#param_v1{name = [F]}, QSize, QUnit]}) ->
    flat_format("quantum(~s, ~p, ~s)", [F, QSize, QUnit]).

make_props([]) ->
    "";
make_props(PL) ->
    [" WITH (",
     string:join(
       [flat_format("~s=~s", [K, make_prop_value(V)]) || {K, V} <- PL], ", "),
     ")"].

make_prop_value(Num) when is_number(Num) ->
    io_lib:format("~p", [Num]);
make_prop_value(Str) ->
    [$',re:replace(Str, "'", "''", [{return, list}, global]), $'].


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

flat_format(F, AA) ->
    lists:flatten(io_lib:format(F, AA)).


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

create_table_test() ->
    roundtrip_ok(
      "create table fafa ("
      " a sint64 not null, b varchar not null, c timestamp not null,"
      " PRIMARY KEY ((quantum(c, 1, m)), c))").

create_table_with_test() ->
    roundtrip_ok(
      "create table fafa ("
      " a sint64 not null, b varchar not null, c timestamp not null,"
      " PRIMARY KEY ((quantum(c, 1, m)), c))"
      " with (x=4, y='3', z='', X='wo''a\"h')").
      %% sql standard uses doubling of single quotes instead of escaping, and '\' is not special

select_single_simple_test() ->
    roundtrip_ok(
      "select a from b where a < 11 and a > 33").
select_multiple_simple_test() ->
    roundtrip_ok(
      "select a, a1 from b where a < 11 and a > 33").
%% select_multiple_ffa_test() ->
%%     roundtrip_ok(
%%       "select avg((a+4)), (avg((a)+4))+2, stddev(x/2 + 2),"
%%       " 3*23+2, 3+23*2,"
%%       " 3+(23*2), (3+23)*2, 3*(23+2), (3*23)+2,"
%%       " (4), (((2))), 5*5, a*2, -8, 8, (8), -8 - 4,-8+3,-8*2, d, (e)"
%%       " from b where a < 11+2 and a > 33").
%%% uncomment when parser gets to skill level 80

%% because of the need to ignore whitespace, case and paren
%% differences, let's convert strings to SQL structure and do the
%% comparisons on those
roundtrip_ok(Text) ->
    SQL = string_to_sql(Text),
    %% ?debugFmt("\n Orig SQL : ~p", [SQL]),
    %% ?debugFmt("\n Orig stmt: \"~s\"", [Text]),
    Text2 = sql_to_string(SQL),
    %% ?debugFmt("\n Converted stmt: \"~s\"", [Text2]),
    ?assertEqual(
       SQL, string_to_sql(Text2)).

string_to_sql(Text) ->
    riak_ql_parser:ql_parse(
      riak_ql_lexer:get_tokens(Text)).

-endif.
