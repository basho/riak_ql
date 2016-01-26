%% -------------------------------------------------------------------
%%
%% Tests for the Lexer
%%
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
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
-module(lexer_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("riak_ql_ddl.hrl").

%% Tests
keywords_1_test_() ->
    Str = "select",
    Got = riak_ql_lexer:get_tokens(Str),
    ?_assertEqual([{select, <<"select">>}], Got).

keywords_2_test_() ->
    Str = "seLEct",
    Got = riak_ql_lexer:get_tokens(Str),
    ?_assertEqual([{select, <<"seLEct">>}], Got).

keywords_3_test_() ->
    Got = riak_ql_lexer:get_tokens("from LiMit Where anD oR"),
    Expected = [
                {from,    <<"from">>},
                {limit,   <<"LiMit">>},
                {where,   <<"Where">>},
                {and_,    <<"anD">>},
                {or_,     <<"oR">>}
               ],
    ?_assertEqual(Expected, Got).

keywords_3a_test_() ->
    Got = riak_ql_lexer:get_tokens("from limit where and or"),
    Expected = [
                {from,    <<"from">>},
                {limit,   <<"limit">>},
                {where,   <<"where">>},
                {and_,    <<"and">>},
                {or_,     <<"or">>}
               ],
    ?_assertEqual(Expected, Got).

keywords_3b_test_() ->
    Got = riak_ql_lexer:get_tokens("FROM LIMIT WHERE AND OR"),
    Expected = [
                {from,    <<"FROM">>},
                {limit,   <<"LIMIT">>},
                {where,   <<"WHERE">>},
                {and_,    <<"AND">>},
                {or_,     <<"OR">>}
               ],
    ?_assertEqual(Expected, Got).

keyword_general_test_() ->
    ?_assertEqual(
       [
        {boolean, <<"boolean">>},
        {boolean, <<"BOOLEAN">>},
        {boolean, <<"booLEan">>}
       ],
       riak_ql_lexer:get_tokens("boolean BOOLEAN booLEan ")
      ).


keyword_int_test_() ->
    ?_assertEqual(
       [{sint64, <<"sint64">>},
        {sint64, <<"SINT64">>},
        {sint64, <<"siNT64">>},
        {sint64, <<"Sint64">>}],
       riak_ql_lexer:get_tokens("sint64 SINT64 siNT64 Sint64")
      ).

keyword_double_test_() ->
    ?_assertEqual(
       [{double, <<"double">>},
        {double, <<"Double">>},
        {double, <<"dOuble">>},
        {double, <<"DOUBLE">>}],
       riak_ql_lexer:get_tokens("double Double dOuble DOUBLE")
      ).

keywords_create_test_() ->
    Got = riak_ql_lexer:get_tokens("create table not null primary key"),
    Expected = [
                {create, <<"create">>},
                {table, <<"table">>},
                {not_, <<"not">>},
                {null, <<"null">>},
                {primary, <<"primary">>},
                {key, <<"key">>}],
    ?_assertEqual(Expected, Got).

words_containing_keywords_test_() ->
    Got = riak_ql_lexer:get_tokens("error or horror and handy andy or "
                                   "zdoublecintxcreateyb or jimmy3boy or jimmy4"),
    Expected = [
                {identifier, <<"error">>},
                {or_,   <<"or">>},
                {identifier, <<"horror">>},
                {and_,  <<"and">>},
                {identifier, <<"handy">>},
                {identifier, <<"andy">>},
                {or_,   <<"or">>},
                {identifier, <<"zdoublecintxcreateyb">>},
                {or_,   <<"or">>},
                {identifier, <<"jimmy3boy">>},
                {or_,   <<"or">>},
                {identifier, <<"jimmy4">>}
               ],
    ?_assertEqual(Expected, Got).

words_containing_digits_test_() ->
    Got = riak_ql_lexer:get_tokens("'sha512' sha 512"),
    Expected = [
                {character_literal, <<"sha512">>},
                {identifier, <<"sha">>},
                {integer, 512}
               ],
    ?_assertEqual(Expected, Got).

nums_test_() ->
    Got = riak_ql_lexer:get_tokens("1 -2 2.0 -2.0 3.3e+3 -3.3e-3 44e4 44e-4 44e+4 44e-0 44e+0 44e0"),
    Expected = [
                {integer, 1},
                {integer, -2},
                {float, 2.0},
                {float, -2.0},
                {float, 3.3e3},
                {float, -0.0033},
                {float, 4.4e5},
                {float, 0.0044},
                {float, 4.4e5},
                {float, 44.0},
                {float, 44.0},
                {float, 44.0}
               ],
    ?_assertEqual(Expected, Got).

floats_in_character_literals_test_() ->
    ?_assertEqual(
        [{character_literal, <<"hello44.4">>}],
        riak_ql_lexer:get_tokens("'hello44.4'")
    ).
negative_floats_in_character_literals_test_() ->
    ?_assertEqual(
        [{character_literal, <<"hello-44.4">>}],
        riak_ql_lexer:get_tokens("'hello-44.4'")
    ).

sci_floats_in_character_literals_test_() ->
    ?_assertEqual(
        [{character_literal, <<"hello4.40000000000000035527e+00">>}],
        riak_ql_lexer:get_tokens("'hello4.40000000000000035527e+00'")
    ).

negative_sci_floats_in_character_literals_test_() ->
    ?_assertEqual(
        [{character_literal, <<"hello-4.40000000000000035527e+00">>}],
        riak_ql_lexer:get_tokens("'hello-4.40000000000000035527e+00'")
    ).

ops_test_() ->
    Got = riak_ql_lexer:get_tokens("> < <> != !~ = =~"),
    Expected = [
                {greater_than_operator, <<">">>},
                {less_than_operator,    <<"<">>},
                {ne,                    <<"<>">>},
                {nomatch,               <<"!=">>},
                {notapprox,             <<"!~">>},
                {equals_operator,       <<"=">>},
                {approx,                <<"=~">>}
               ],
    ?_assertEqual(Expected, Got).

brackets_test_() ->
    Got = riak_ql_lexer:get_tokens(" ( )"),
    Expected = [
                {left_paren,  <<"(">>},
                {right_paren, <<")">>}
               ],
    ?_assertEqual(Expected, Got).

unicode_character_literal_test_() ->
    Got = riak_ql_lexer:get_tokens("'πίτσα пицца ピザ 比萨'"),
    Expected = [
                {character_literal, <<"πίτσα пицца ピザ 比萨">>}
               ],
    ?_assertEqual(Expected, Got).

unicode_identifier_test_() ->
    ?_assertException(
        error,
        unicode_in_identifier,
        riak_ql_lexer:get_tokens("πίτσα пицца ピザ 比萨")
    ).

unicode_quoted_test_() ->
    ?_assertException(
        error,
        unicode_in_quotes,
        riak_ql_lexer:get_tokens("\"helピザlo\"")
    ).

inner_zero_test_() ->
    Got = riak_ql_lexer:get_tokens("foo01 \"foo01\" 'foo01'"),
    Expected = [
                {identifier, <<"foo01">>},
                {identifier, <<"foo01">>},
                {character_literal, <<"foo01">>}
               ],
    ?_assertEqual(Expected, Got).

keywords_in_words_test_() ->
    Got = riak_ql_lexer:get_tokens("mydouble mysint64 myboolean mycreate myany"),
    Expected = [
                {identifier, <<"mydouble">>},
                {identifier, <<"mysint64">>},
                {identifier, <<"myboolean">>},
                {identifier, <<"mycreate">>},
                {identifier, <<"myany">>}
               ],
    ?_assertEqual(Expected, Got).

not_a_date_test_() ->
    Got = riak_ql_lexer:get_tokens("'ergle'"),
    Expected = [
                {character_literal, <<"ergle">>}
               ],
    ?_assertEqual(Expected, Got).

multiple_single_quotes_test_() ->
    Got = riak_ql_lexer:get_tokens("'user_1' 'San Fierro' 'klingon''name'"),
    Expected = [
                {character_literal, <<"user_1">>},
                {character_literal, <<"San Fierro">>},
                {character_literal, <<"klingon'name">>}
               ],
    ?_assertEqual(Expected, Got).

double_quote_1_test_() ->
    Got = riak_ql_lexer:get_tokens("\" yardle hoop !@#$%^&*() _ -\""),
    Expected = [
                {identifier, <<" yardle hoop !@#$%^&*() _ -">>}
               ],
    ?_assertEqual(Expected, Got).

double_quote_2_test_() ->
    Got = riak_ql_lexer:get_tokens("\"first quote\" \"second quote\""),
    Expected = [
                {identifier, <<"first quote">>},
                {identifier, <<"second quote">>}
               ],
    ?_assertEqual(Expected, Got).

regex_1_test_() ->
    Got = riak_ql_lexer:get_tokens("/*./"),
    Expected = [
                {regex, <<"/*./">>}
               ],
    ?_assertEqual(Expected, Got).

regex_2_test_() ->
    Got = riak_ql_lexer:get_tokens("/^*./i"),
    Expected = [
                {regex, <<"/^*./i">>}
               ],
    io:format("Expected is ~p~n", [Expected]),
    io:format("Got is ~p~n", [Got]),
    ?_assertEqual(Expected, Got).

regex_3_test_() ->
    Got = riak_ql_lexer:get_tokens("/*./ or /erkle/"),
    Expected = [
                {regex, <<"/*./">>},
                {or_,   <<"or">>},
                {regex, <<"/erkle/">>}
               ],
    ?_assertEqual(Expected, Got).

chars_test_() ->
    Got = riak_ql_lexer:get_tokens("r_t , ee where"),
    Expected = [
                {identifier, <<"r_t">>},
                {comma, <<",">>},
                {identifier, <<"ee">>},
                {where, <<"where">>}
               ],
    io:format("Expected is ~p~n", [Expected]),
    io:format("Got is ~p~n", [Got]),
    ?_assertEqual(Expected, Got).

arithmatic_test_() ->
    Got = riak_ql_lexer:get_tokens(" + - * / "),
    Expected = [
                {plus_sign,     <<"+">>},
                {minus_sign,    <<"-">>},
                {asterisk, <<"*">>},
                {solidus,       <<"/">>}
               ],
    ?_assertEqual(Expected, Got).

semicolon_test_() ->
    Expected = [{semicolon, <<";">>}],
    Got = riak_ql_lexer:get_tokens(";"),
    ?_assertEqual(Expected, Got).

general_test_() ->
    Got = riak_ql_lexer:get_tokens("select v from r_t where time > '23 April 63 1:2:3'"),
    Expected = [
                {select, <<"select">>},
                {identifier,  <<"v">>},
                {from, <<"from">>},
                {identifier, <<"r_t">>},
                {where, <<"where">>},
                {identifier, <<"time">>},
                {greater_than_operator, <<">">>},
                {character_literal, <<"23 April 63 1:2:3">>}
               ],
    io:format("Expected is ~p~n", [Expected]),
    io:format("Got is ~p~n", [Got]),
    ?_assertEqual(Expected, Got).

timeseries_test_() ->
    Got = riak_ql_lexer:get_tokens("CREATE TABLE Geo ("
                                   ++ "geohash varchar not_null, "
                                   ++ "user varchar not_null, "
                                   ++ "time timestamp not_null, "
                                   ++ "weather varchar not_null, "
                                   ++ "temperature double not_null, "
                                   ++ "PRIMARY KEY ((geohash, quantum(time, 15, m), time, user)"),
    Expected = [
                {create, <<"CREATE">>},
                {table, <<"TABLE">>},
                {identifier,<<"Geo">>},
                {left_paren, <<"(">>},
                {identifier,<<"geohash">>},
                {varchar, <<"varchar">>},
                {identifier,<<"not_null">>},
                {comma, <<",">>},
                {identifier,<<"user">>},
                {varchar, <<"varchar">>},
                {identifier,<<"not_null">>},
                {comma, <<",">>},
                {identifier,<<"time">>},
                {timestamp, <<"timestamp">>},
                {identifier,<<"not_null">>},
                {comma, <<",">>},
                {identifier,<<"weather">>},
                {varchar, <<"varchar">>},
                {identifier,<<"not_null">>},
                {comma, <<",">>},
                {identifier,<<"temperature">>},
                {double, <<"double">>},
                {identifier,<<"not_null">>},
                {comma, <<",">>},
                {primary, <<"PRIMARY">>},
                {key, <<"KEY">>},
                {left_paren, <<"(">>},
                {left_paren, <<"(">>},
                {identifier,<<"geohash">>},
                {comma, <<",">>},
                {quantum, <<"quantum">>},
                {left_paren, <<"(">>},
                {identifier,<<"time">>},
                {comma, <<",">>},
                {integer,15},
                {comma, <<",">>},
                {identifier,<<"m">>},
                {right_paren, <<")">>},
                {comma, <<",">>},
                {identifier,<<"time">>},
                {comma, <<",">>},
                {identifier,<<"user">>},
                {right_paren, <<")">>}
               ],
    ?_assertEqual(Expected, Got).

unquoted_identifiers_test_() ->
    String = "cats = be a st",
    Got = riak_ql_lexer:get_tokens(String),
    Expected = [
                {identifier, <<"cats">>},
                {equals_operator, <<"=">>},
                {identifier, <<"be">>},
                {identifier, <<"a">>},
                {identifier, <<"st">>}
               ],
    ?_assertEqual(Expected, Got).

symbols_in_identifier_1_test_() ->
    ?_assertError(
        <<"Unexpected token '^'.">>,
        riak_ql_lexer:get_tokens(
            "CREATE TABLE ^ ("
            "time TIMESTAMP NOT NULL, "
            "family VARCHAR NOT NULL, "
            "series VARCHAR NOT NULL, "
            "PRIMARY KEY "
            " ((family, series, quantum(time, 15, 's')), family, series, time))")
    ).

symbols_in_identifier_2_test_() ->
    ?_assertError(
        <<"Unexpected token '&'.">>,
        riak_ql_lexer:get_tokens("klsdafj kljfd (*((*& 89& 8KHH kJHkj hKJH K K")
    ).

symbols_in_identifier_3_test_() ->
    ?_assertError(
        <<"Unexpected token '$'.">>,
        riak_ql_lexer:get_tokens(
            "CREATE TABLE mytable ("
            "time TIMESTAMP NOT NULL, "
            "family $ NOT NULL, "
            "series VARCHAR NOT NULL, "
            "PRIMARY KEY "
            " ((family, series, quantum(time, 15, 's')), family, series, time))")
    ).

symbols_in_identifier_4_test_() ->
    ?_assertError(
        <<"Unexpected token ']'.">>,
        riak_ql_lexer:get_tokens("select ] from a")
    ).
