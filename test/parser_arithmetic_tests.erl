-module(parser_arithmetic_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("riak_ql_ddl.hrl").

-define(sql_comp_assert(String, Expected),
        Exp2 = fix(Expected),
        Toks = riak_ql_lexer:get_tokens(String),
        Got = riak_ql_parser:parse(Toks),
 %% io:format(standard_error, "~p~n~p~n", [Exp2, Got]),
        ?assertEqual({ok, Exp2}, Got)).

fix(#riak_sql_v1{'FROM' = F} = Expected) ->
    case F of
        {regex, _} -> Expected;
        {list,  _} -> Expected;
        _          -> Mod = riak_ql_ddl:make_module_name(F),
                      Expected#riak_sql_v1{helper_mod = Mod}
    end;
fix(Other) ->
    Other.

select_arithmetic_test() ->
    ?sql_comp_assert("select temperature + 1 from details",
                     #riak_sql_v1{'SELECT' = #riak_sel_clause_v1{clause = [
                                              {'+',
                                               {identifier, <<"temperature">>},
                                               {integer, 1}
                                              }
                                             ]},
                                  'FROM'    = <<"details">>,
                                  'WHERE'   = []
                                 }).


window_aggregate_fn_arithmetic_1_test() ->
    ?sql_comp_assert("select aVg(temperature) + 1 - 2 * 3 / 4 from details",
                     #riak_sql_v1{'SELECT' =
                                      #riak_sel_clause_v1{clause = [
                                       {'-',
                                        {'+',
                                         {{window_agg_fn,'AVG'},
                                          [{identifier,[<<"temperature">>]}]},
                                         {integer,1}},
                                        {'/',
                                         {'*',
                                          {integer,2},{integer,3}},
                                         {integer,4}}}
                                      ]},
                                  'FROM'    = <<"details">>,
                                  'WHERE'   = []
                                 }).

arithmetic_precedence_test() ->
    ?sql_comp_assert("select 1 * 2 + 3 / 4 - 5 * 6 from dual",
                     #riak_sql_v1{'SELECT' = #riak_sel_clause_v1{clause = [{'-',
                                               {'+',
                                                {'*', {integer,1}, {integer,2}},
                                                {'/', {integer,3}, {integer,4}}
                                               },
                                               {'*', {integer,5}, {integer,6}}
                                               }]},
                                  'FROM' = <<"dual">>,
                                  'WHERE' = []
                                 }).

parens_precedence_test() ->
    ?sql_comp_assert("select 1 * (2 + 3) / (4 - 5) * 6 from dual",
                     #riak_sql_v1{
                        'SELECT' = 
                            #riak_sel_clause_v1{
                               clause = [{'*',
                                          {'/',
                                           {'*', {integer,1},
                                            {'+',{integer,2}, {integer,3}}},
                                           {'-',{integer,4}, {integer,5}}},
                                          {integer,6}}]},
                                  'FROM' = <<"dual">>,
                                  'WHERE' = []
                                 }).

negated_parens_test() ->
        ?sql_comp_assert("select - (2 + 3) from dual",
                     #riak_sql_v1{
                        'SELECT' = 
                            #riak_sel_clause_v1{
                               clause = [{negate,
                                          {expr, 
                                           {'+', {integer,2}, {integer,3}}}}
                                        ]},
                                  'FROM' = <<"dual">>,
                                  'WHERE' = []
                                 }).
