-module(parser_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("riak_ql_ddl.hrl").

not_null_white_space_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time TIMESTAMP NOT                NULL, "
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
       {ok, #ddl_v1{}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

create_table_white_space_test() ->
    Table_def =
        "CREATE               \tTABLE temperatures ("
        "time TIMESTAMP NOT NULL, "
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
       {ok, #ddl_v1{}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

primary_key_white_space_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time TIMESTAMP NOT NULL, "
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY               \t  KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
       {ok, #ddl_v1{}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

function_arity_0_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun() = a"))
      ).

function_identifier_arity_1_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun(a) = a"))
      ).

function_identifier_arity_2_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun(a, b) = a"))
      ).

function_val_arity_1_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a') = a"))
      ).

function_val_arity_2_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 'b') = a"))
      ).

function_val_arity_3_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 'b', 'c') = a"))
      ).

function_val_arity_10_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b') = a"))
      ).

function_val_and_identifier_mix_1_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 10, b, 3.5) = a"))
      ).

function_val_and_identifier_mix_2_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 10, b, 3.5, true) = a"))
      ).

function_val_and_identifier_mix_3_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 10, b, 3.5, false) = a"))
      ).

function_call_error_message_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<"Function not supported - 'myfun'.">>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a') = a"))
      ).

function_as_arg_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser,
                <<"Function not supported - 'herfun'.">>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun(hisfun(herfun(a))) = 'a'"))
      ).

%% RTS-645
flubber_test() ->
    ?assertEqual(
       {error, {0, riak_ql_parser,
                <<"Used f as a measure of time in 1f. Only s, m, h and d are allowed.">>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM ts_X_subquery "
                              "WHERE d > 0 AND d < 1 f = 'f' "
                              "AND s='s' AND ts > 0 AND ts < 100"))
      ).

key_fields_must_exist_1_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time TIMESTAMP NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertEqual(
       {error, {0, riak_ql_parser, <<"Primary key fields do not exist (family).">>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

key_fields_must_exist_2_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time TIMESTAMP NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertEqual(
       {error, {0, riak_ql_parser, <<"Primary key fields do not exist (family, series).">>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

key_fields_must_exist_3_test() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "family VARCHAR NOT NULL, "
        "series VARCHAR NOT NULL, "
        "PRIMARY KEY "
        " ((family, series, quantum(time, 15, 's')), family, series, time))",
    ?assertMatch(
       {error, {0, riak_ql_parser, <<"Primary key fields do not exist (time).">>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(Table_def))
      ).

time_unit_seconds_test() ->
    ?assertMatch(
       {ok, ?SQL_SELECT{
               'WHERE' = [{and_,
                           {'<',<<"time">>,{integer,20 * 1000}},
                           {'>',<<"time">>,{integer,10 * 1000}}}]
              }},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE time > 10s AND time < 20s"))
      ).

time_unit_minutes_test() ->
    ?assertMatch(
       {ok, ?SQL_SELECT{
               'WHERE' = [{and_,
                           {'<',<<"time">>,{integer,20 * 60 * 1000}},
                           {'>',<<"time">>,{integer,10 * 60 * 1000}}}]
              }},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE time > 10m AND time < 20m"))
      ).

time_unit_seconds_and_minutes_test() ->
    ?assertMatch(
       {ok, ?SQL_SELECT{
               'WHERE' = [{and_,
                           {'<',<<"time">>,{integer,20 * 60 * 1000}},
                           {'>',<<"time">>,{integer,10 * 1000}}}]
              }},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE time > 10s AND time < 20m"))
      ).

time_unit_hours_test() ->
    ?assertMatch(
       {ok, ?SQL_SELECT{
               'WHERE' = [{and_,
                           {'<',<<"time">>,{integer,20 * 60 * 60 * 1000}},
                           {'>',<<"time">>,{integer,10 * 60 * 60 * 1000}}}]
              }},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE time > 10h AND time < 20h"))
      ).

time_unit_days_test() ->
    ?assertMatch(
       {ok, ?SQL_SELECT{
               'WHERE' = [{and_,
                           {'<',<<"time">>,{integer,20 * 60 * 60 * 24 * 1000}},
                           {'>',<<"time">>,{integer,10 * 60 * 60 * 24 * 1000}}}]
              }},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE time > 10d AND time < 20d"))
      ).

time_unit_invalid_1_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE time > 10y AND time < 20y"))
      ).

time_unit_invalid_2_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE time > 10mo AND time < 20mo"))
      ).

time_unit_whitespace_test() ->
    ?assertMatch(
       {ok, ?SQL_SELECT{
               'WHERE' = [{and_,
                           {'<',<<"time">>,{integer,20 * 60 * 60 * 24 * 1000}},
                           {'>',<<"time">>,{integer,10 * 60 * 60 * 24 * 1000}}}]
              }},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE time > 10   d AND time < 20\td"))
      ).

time_unit_case_insensitive_test() ->
    ?assertMatch(
       {ok, ?SQL_SELECT{ }},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE time > 10S "
                              "AND time < 20M AND time > 15H and time < 4D"))
      ).

left_hand_side_literal_equals_test() ->
    ?assertMatch(
       {ok, ?SQL_SELECT{
               'WHERE' = [{'=', <<"age">>, {integer, 10}}]
              }},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE 10 = age"))
      ).

left_hand_side_literal_not_equals_test() ->
    ?assertMatch(
       {ok, ?SQL_SELECT{
               'WHERE' = [{'!=', <<"age">>, {integer, 10}}]
              }},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytable WHERE 10 != age"))
      ).

%% RTS-788
%% an infinite loop was occurring when two where clauses were the same
%% i.e. time = 10 and time 10
infinite_loop_test_() ->
    {timeout, 0.2,
     fun() ->
             ?assertMatch(
                {ok, _},
                riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                                       "Select myseries, temperature from GeoCheckin2 "
                                       "where time > 1234567 and time > 1234567 "
                                       "and myfamily = 'family1' and myseries = 'series1' "))
               )
     end}.

remove_duplicate_clauses_1_test() ->
    ?assertEqual(
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytab "
                              "WHERE time > 1234567 ")),
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytab "
                              "WHERE time > 1234567 AND time > 1234567"))
      ).

remove_duplicate_clauses_2_test() ->
    ?assertEqual(
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytab "
                              "WHERE time > 1234567 ")),
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytab "
                              "WHERE time > 1234567 AND time > 1234567 AND time > 1234567 "))
      ).

remove_duplicate_clauses_3_test() ->
    ?assertEqual(
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytab "
                              "WHERE time > 1234567 ")),
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytab "
                              "WHERE time > 1234567 AND time > 1234567 OR time > 1234567 "))
      ).

remove_duplicate_clauses_4_test() ->
    ?assertEqual(
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytab "
                              "WHERE time > 1234567 ")),
       riak_ql_parser:parse(riak_ql_lexer:get_tokens(
                              "SELECT * FROM mytab "
                              "WHERE time > 1234567 AND (time > 1234567 OR time > 1234567) "))
      ).

%% This fails. de-duping does not yet go through the entire tree and
%% pull out duplicates
%% remove_duplicate_clauses_5_test() ->
%%   ?assertEqual(
%%         riak_ql_parser:parse(riak_ql_lexer:get_tokens(
%%             "SELECT * FROM mytab "
%%             "WHERE time > 1234567 "
%%             "AND (localtion > 'derby' OR time > 'sheffield') "
%%             "AND weather = 'raining' ")),
%%         riak_ql_parser:parse(riak_ql_lexer:get_tokens(
%%             "SELECT * FROM mytab "
%%             "WHERE time > 1234567 "
%%             "AND (localtion > 'derby' OR time > 'sheffield') "
%%             "AND weather = 'raining' "
%%             "AND time > 1234567 "))
%%     ).
