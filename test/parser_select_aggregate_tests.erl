-module(parser_select_aggregate_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

window_aggregate_fn_1_test() ->
    ?sql_comp_assert("select avg(temp) from details",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                                                          {{window_agg_fn, 'AVG'},
                                                                           [{identifier, [<<"temp">>]}]}
                                                                         ]},
                                 'FROM'    = <<"details">>,
                                 'WHERE'   = []
                                }).

window_aggregate_fn_1a_test() ->
    ?sql_comp_assert("select mean(temp) from details",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                                                          {{window_agg_fn, 'MEAN'},
                                                                           [{identifier, [<<"temp">>]}]}
                                                                         ]},
                                 'FROM'    = <<"details">>,
                                 'WHERE'   = []
                                }).

window_aggregate_fn_2_test() ->
    ?sql_comp_assert("select avg(temp), sum(counts) from details",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                                                          {{window_agg_fn, 'AVG'},
                                                                           [{identifier, [<<"temp">>]}]},
                                                                          {{window_agg_fn, 'SUM'},
                                                                           [{identifier, [<<"counts">>]}]}
                                                                         ]},
                                 'FROM'    = <<"details">>,
                                 'WHERE'   = []
                                }).

window_aggregate_fn_wildcard_count_test() ->
    ?sql_comp_assert("select count(*) from details",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                                                          {{window_agg_fn, 'COUNT'},
                                                                           [{identifier, [<<"*">>]}]}
                                                                         ]},
                                 'FROM'    = <<"details">>,
                                 'WHERE'   = []
                                }).

window_aggregate_fn_capitalisation_test() ->
    ?sql_comp_assert("select aVg(temp) from details",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                                                          {{window_agg_fn, 'AVG'},
                                                                           [{identifier, [<<"temp">>]}]}
                                                                         ]},
                                 'FROM'    = <<"details">>,
                                 'WHERE'   = []
                                }).

window_aggregate_fn_all_funs_test() ->
    ?sql_comp_assert("select avg(temp), sum(counts), count(counts), min(counts), " ++
                         "max(counts), stddev(counts) from details",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                                                          {{window_agg_fn, 'AVG'},
                                                                           [{identifier, [<<"temp">>]}]},
                                                                          {{window_agg_fn, 'SUM'},
                                                                           [{identifier, [<<"counts">>]}]},
                                                                          {{window_agg_fn, 'COUNT'},
                                                                           [{identifier, [<<"counts">>]}]},
                                                                          {{window_agg_fn, 'MIN'},
                                                                           [{identifier, [<<"counts">>]}]},
                                                                          {{window_agg_fn, 'MAX'},
                                                                           [{identifier, [<<"counts">>]}]},
                                                                          {{window_agg_fn, 'STDDEV'},
                                                                           [{identifier, [<<"counts">>]}]}
                                                                         ]},
                                 'FROM'    = <<"details">>,
                                 'WHERE'   = []
                                }).

window_aggregate_fn_arithmetic_2_test() ->
    ?sql_comp_assert("select aVg(temperature) + count(temperature) from details",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                                                          {'+',
                                                                           {{window_agg_fn, 'AVG'},
                                                                            [{identifier, [<<"temperature">>]}]},
                                                                           {{window_agg_fn, 'COUNT'},
                                                                            [{identifier, [<<"temperature">>]}]}}
                                                                         ]},
                                 'FROM'    = <<"details">>,
                                 'WHERE'   = []
                                }).

window_aggregate_fn_arithmetic_3_test() ->
    ?sql_comp_assert(
        "select aVg(temperature + 1) + count(temperature / distance) from details",
        ?SQL_SELECT{
             'SELECT' = #riak_sel_clause_v1{ clause = [
                  {'+',
                      {{window_agg_fn, 'AVG'}, [{'+', {identifier, <<"temperature">>}, {integer, 1}}]},
                      {{window_agg_fn, 'COUNT'}, [{'/', {identifier, <<"temperature">>}, {identifier, <<"distance">>}}]}
                  }]
                },
             'FROM'    = <<"details">>,
             'WHERE'   = []
    }).

%%
%% TS 1.1 fail tests
%%

window_aggregate_fn_not_supported_test() ->
    ?sql_comp_fail("select bingo(temp) from details").

window_aggregate_fn_wildcard_fail_test() ->
    ?sql_comp_fail("select avg(*) from details").
