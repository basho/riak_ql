-module(riak_ql_interpolator).

-include("riak_ql_sql.hrl").

-export([interpolate/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% null case, no interps
interpolate(Stmt = #riak_sql_v1{}, []) ->
    Stmt;

% limit is easy, top-level
interpolate(Stmt = #riak_sql_v1{'LIMIT' = {interp, InterpName}}, Interpolations) ->
    InterpValue = find_value(InterpName, Interpolations),
    InterpolatedStmt = Stmt#riak_sql_v1{'LIMIT' = InterpValue},
    interpolate(InterpolatedStmt, Interpolations);

% where requires more introspection
interpolate(Stmt = #riak_sql_v1{'WHERE' = [WhereTree]}, Interpolations) ->
    InterpolatedWhere = interpolate_where(WhereTree, Interpolations),
    Stmt#riak_sql_v1{'WHERE' = [InterpolatedWhere]}.

interpolate_where({Cond, A, B}, Interpolations) ->
    {Cond, interpolate_where(A, Interpolations), interpolate_where(B, Interpolations)};
interpolate_where({interp, InterpName}, Interpolations) ->
    find_value(InterpName, Interpolations);
interpolate_where(OtherThing, _Interpolations) ->
    OtherThing.

find_value(_Name, []) ->
    notfound;
find_value(Name, [{Name, Value}|_Rest]) ->
    parse_value(Value);
find_value(Name, [_NotFound | Rest]) ->
    find_value(Name, Rest).

parse_value({binary, Value}) ->
    {word, <<Value>>};
parse_value({integer, Value}) ->
    {int, Value};
parse_value({numeric, ValueStr}) ->
    case list_to_integer(ValueStr) of
        badarg -> {float, list_to_float(ValueStr)};
        SomeInteger ->
            {int, SomeInteger}
    end;
parse_value({timestamp, Value}) ->
    {integer, Value};
parse_value({boolean, Value}) ->
    Value.


-ifdef(TEST).
null_interpolation_test_() ->
    Stmt = #riak_sql_v1{'SELECT' = [[<<"*">>]],
                        'FROM' = <<"argle">>},
    ?_assertEqual(Stmt, interpolate(Stmt, [])).

where_interpolation_test_() ->
    GivenStmt = #riak_sql_v1{'SELECT' = [[<<"*">>]],
                        'FROM' = <<"argle">>,
                        'WHERE' = [
                                   {'>', <<"time">>, {interp, "where_param"}}
                                  ]},
    GivenInterp = [{"where_param", {integer, 12345}}],
    ExpectedStmt = #riak_sql_v1{'SELECT' = [[<<"*">>]],
                        'FROM' = <<"argle">>,
                        'WHERE' = [
                                   {'>', <<"time">>, {int, 12345}}
                                  ]},
    ?_assertEqual(ExpectedStmt, interpolate(GivenStmt, GivenInterp)).
    

limit_interpolation_test_() ->
    GivenStmt = #riak_sql_v1{'SELECT' = [[<<"*">>]],
                             'FROM' = <<"argle">>,
                             'WHERE' = [
                                        {'>', <<"time">>, {int, 12345}}
                                        ],
                             'LIMIT' = {interp, "limit_param"}},
    GivenInterp = [{"limit_param", {integer, 12345}}],
    ExpectedStmt = #riak_sql_v1{'SELECT' = [[<<"*">>]],
                             'FROM' = <<"argle">>,
                             'WHERE' = [
                                        {'>', <<"time">>, {int, 12345}}
                                        ],
                             'LIMIT' = {int, 12345}},
    ?_assertEqual(ExpectedStmt, interpolate(GivenStmt, GivenInterp)).
-endif.
