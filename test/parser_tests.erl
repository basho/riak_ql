-module(parser_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").


select_sql_case_insensitive_1_test() ->
    ?sql_comp_assert_match("SELECT * from argle",
                           ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]}}).

select_sql_case_insensitive_2_test() ->
    ?sql_comp_assert_match("seLEct * from argle",
                           ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]}}).


sql_first_char_is_newline_test() ->
    ?sql_comp_assert_match("\nselect * from argle",
                           ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]}}).



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

concatenated_unquoted_strings_test() ->
    String = "select * from response_times where cats = be a st",
    Expected = error,
    Got = case riak_ql_parser:parse(riak_ql_lexer:get_tokens(String)) of
              {error, _Err} ->
                  error;
              {ok, Other} -> {should_not_compile, Other}
          end,
    ?assertEqual(Expected, Got).

%%
%% Regression tests
%%

rts_433_regression_test() ->
    ?sql_comp_assert("select * from HardDrivesV14 where date >= 123 " ++
                         "and date <= 567 " ++
                         "and family = 'Hitachi HDS5C4040ALE630' " ++
                         "and series = 'true'",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{identifier, [<<"*">>]}]},
                                 'FROM'   = <<"HardDrivesV14">>,
                                 'WHERE'  = [
                                             {and_,
                                              {'=', <<"series">>, {binary, <<"true">>}},
                                              {and_,
                                               {'=', <<"family">>, {binary, <<"Hitachi HDS5C4040ALE630">>}},
                                               {and_,
                                                {'<=', <<"date">>, {integer, 567}},
                                                {'>=', <<"date">>, {integer, 123}}
                                               }
                                              }
                                             }
                                            ]
                                }).
