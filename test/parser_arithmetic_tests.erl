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

fix(?SQL_SELECT{'FROM' = F} = Expected) ->
    case F of
        {regex, _} -> Expected;
        {list,  _} -> Expected;
        _          -> Mod = riak_ql_ddl:make_module_name(F),
                      Expected?SQL_SELECT{helper_mod = Mod}
    end;
fix(Other) ->
    Other.

-define(sql_comp_fail(QL_string),
        Toks = riak_ql_lexer:get_tokens(QL_string),
        Got = riak_ql_parser:parse(Toks),
        ?assertMatch({error, _}, Got)).

select_arithmetic_test() ->
    ?sql_comp_assert("select temperature + 1 from details",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [
                                              {'+',
                                               {identifier, <<"temperature">>},
                                               {integer, 1}
                                              }
                                             ]},
                                  'FROM'    = <<"details">>,
                                  'WHERE'   = []
                                 }).


window_aggregate_fn_arithmetic_1_test() ->
    ?sql_comp_fail("select aVg(temperature) + 1 - 2 * 3 / 4 from details").

arith_inside_aggregate_fn_test() ->
    ?sql_comp_fail("select aVg((temperature * 2) + 32) from details").

arith_with_multiple_agg_fn_test() ->
    ?sql_comp_fail("select count(x) + 1 / avg(y) from details").

arithmetic_precedence_test() ->
    ?sql_comp_assert("select 1 * 2 + 3 / 4 - 5 * 6 from dual",
                     ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{clause = [{'-',
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
                     ?SQL_SELECT{
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
                     ?SQL_SELECT{
                        'SELECT' =
                            #riak_sel_clause_v1{
                               clause = [{negate,
                                          {expr,
                                           {'+', {integer,2}, {integer,3}}}}
                                        ]},
                                  'FROM' = <<"dual">>,
                                  'WHERE' = []
                                 }).

no_functions_in_where_test() ->
    ?sql_comp_fail("select * from dual where sin(4) > 4").
