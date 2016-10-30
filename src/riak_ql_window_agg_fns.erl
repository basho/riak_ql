%% -------------------------------------------------------------------
%%
%% riak_ql_window_agg_fns: implementation of Windows Aggregation Fns
%%                         for the query runner
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
-module(riak_ql_window_agg_fns).


-export(['COUNT'/2, 'SUM'/2, 'AVG'/2, 'MEAN'/2, 'MIN'/2, 'MAX'/2, 'STDDEV'/2, 'STDDEV_POP'/2, 'STDDEV_SAMP'/2]).
-export([add/2, divide/2, multiply/2, subtract/2]).
-export([finalise/2]).
-export([start_state/1]).
-export([fn_arity/1]).
-export([fn_type_signature/2]).

-define(SQL_NULL, []).

-type aggregate_function() :: 'COUNT' | 'SUM' | 'AVG' |'MEAN' | 'MIN' | 'MAX' | 'STDDEV' | 'STDDEV_POP' | 'STDDEV_SAMP'.

-include("riak_ql_ddl.hrl").


-spec fn_type_signature(aggregate_function(), Args::[riak_ql_ddl:simple_field_type()]) ->
        riak_ql_ddl:simple_field_type().
fn_type_signature('AVG', [double]) -> double;
fn_type_signature('AVG', [sint64]) -> double;
fn_type_signature('AVG', [timestamp]) -> double;
fn_type_signature('COUNT', [_]) -> sint64;
fn_type_signature('MAX', [double]) -> double;
fn_type_signature('MAX', [sint64]) -> sint64;
fn_type_signature('MAX', [timestamp]) -> sint64;
fn_type_signature('MEAN', Args) -> fn_type_signature('AVG', Args);
fn_type_signature('MIN', [double]) -> double;
fn_type_signature('MIN', [sint64]) -> sint64;
fn_type_signature('MIN', [timestamp]) -> sint64;
fn_type_signature('STDDEV', Args) -> fn_type_signature('STDDEV_SAMP', Args);
fn_type_signature('STDDEV_POP', [double]) -> double;
fn_type_signature('STDDEV_POP', [sint64]) -> double;
fn_type_signature('STDDEV_POP', [timestamp]) -> double;
fn_type_signature('STDDEV_SAMP', [double]) -> double;
fn_type_signature('STDDEV_SAMP', [sint64]) -> double;
fn_type_signature('STDDEV_SAMP', [timestamp]) -> double;
fn_type_signature('SUM', [double]) -> double;
fn_type_signature('SUM', [sint64]) -> sint64;
fn_type_signature('SUM', [timestamp]) -> sint64;
fn_type_signature(Fn, Args) ->
    {error, {argument_type_mismatch, Fn, Args}}.

%%
fn_arity(_FnName) -> 1.

%% Get the initial accumulator state for the aggregation.
-spec start_state(aggregate_function()) ->
    any().
start_state('AVG')    -> ?SQL_NULL;
start_state('MEAN')   -> start_state('AVG');
start_state('COUNT')  -> 0;
start_state('MAX')    -> ?SQL_NULL;
start_state('MIN')    -> ?SQL_NULL;
start_state('STDDEV') -> start_state_stddev();
start_state('STDDEV_POP') -> start_state_stddev();
start_state('STDDEV_SAMP') -> start_state_stddev();
start_state('SUM')    -> ?SQL_NULL;
start_state(_)        -> stateless.

%%
start_state_stddev() ->
    {0, 0.0, 0.0}.

%% Calculate the final results using the accumulated result.
-spec finalise(aggregate_function(), any()) -> any().
finalise(_, ?SQL_NULL) ->
    ?SQL_NULL;
finalise('MEAN', State) ->
    finalise('AVG', State);
finalise('AVG', {N, Acc})  ->
    Acc / N;
finalise(Stddev, {N, _, _}) when (Stddev == 'STDDEV' orelse Stddev == 'STDDEV_POP' orelse Stddev == 'STDDEV_SAMP')  andalso N < 2 ->
    % STDDEV_POP must have two or more values to or return NULL
    ?SQL_NULL;
finalise('STDDEV', State) ->
    finalise('STDDEV_SAMP', State);
finalise('STDDEV_POP', {N, _, Q}) ->
    math:sqrt(Q / N);
finalise('STDDEV_SAMP', {N, _, Q}) ->
    math:sqrt(Q / (N-1));
finalise(_Fn, Acc) ->
    Acc.

%% Group functions (avg, mean etc). These can only appear as top-level
%% expressions in SELECT part, and there can be only one in a query.
%% Can take an Expr that includes the column identifier and some static
%% values.
%%
%% Incrementally operates on chunks, needs to carry state.

'COUNT'(?SQL_NULL, State) ->
    State;
'COUNT'(_, State) ->
    State + 1.

'SUM'(Arg, State) when is_number(Arg), is_number(State) ->
    Arg + State;
'SUM'(?SQL_NULL, State) ->
    State;
'SUM'(Arg, ?SQL_NULL) ->
    Arg.

'MEAN'(Arg, State) ->
    'AVG'(Arg, State).

'AVG'(Arg, {N, Acc}) when is_number(Arg) ->
    {N + 1, Acc + Arg};
'AVG'(Arg, ?SQL_NULL) when is_number(Arg) ->
    {1, Arg};
'AVG'(?SQL_NULL, {_,_} = State) ->
    State;
'AVG'(?SQL_NULL, ?SQL_NULL) ->
    ?SQL_NULL.

'MIN'(Arg, ?SQL_NULL) -> Arg;
'MIN'(Arg, State) when Arg < State -> Arg;
'MIN'(_, State) -> State.

'MAX'(Arg, ?SQL_NULL) when is_number(Arg) -> Arg;
'MAX'(?SQL_NULL, Arg) when is_number(Arg) -> Arg;
'MAX'(?SQL_NULL, ?SQL_NULL) -> ?SQL_NULL;
'MAX'(Arg, State) when Arg > State -> Arg;
'MAX'(_, State) -> State.

'STDDEV'(Arg, State) ->
    'STDDEV_POP'(Arg, State).

'STDDEV_POP'(Arg, {N_old, A_old, Q_old}) when is_number(Arg) ->
    %% A and Q are those in https://en.wikipedia.org/wiki/Standard_deviation#Rapid_calculation_methods
    N = N_old + 1,
    A = A_old + (Arg - A_old) / N,
    Q = Q_old + (Arg - A_old) * (Arg - A),
    {N, A, Q};
'STDDEV_POP'(_, State) ->
    State.

'STDDEV_SAMP'(Arg, State) ->
    'STDDEV_POP'(Arg, State).

%%
add(?SQL_NULL, _) -> ?SQL_NULL;
add(_, ?SQL_NULL) -> ?SQL_NULL;
add(A, B)         -> A + B.

%%
divide(?SQL_NULL, _) -> ?SQL_NULL;
divide(_, ?SQL_NULL) -> ?SQL_NULL;
divide(_, 0)         -> error(divide_by_zero);
divide(_, 0.0)       -> error(divide_by_zero);
divide(A, B) when is_integer(A) andalso is_integer(B)
                     -> A div B;
divide(A, B)         -> A / B.

%%
multiply(?SQL_NULL, _) -> ?SQL_NULL;
multiply(_, ?SQL_NULL) -> ?SQL_NULL;
multiply(A, B)         -> A * B.

%%
subtract(?SQL_NULL, _) -> ?SQL_NULL;
subtract(_, ?SQL_NULL) -> ?SQL_NULL;
subtract(A, B)         -> A - B.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

stddev_pop_test() ->
    State0 = start_state('STDDEV_POP'),
    Data = [
            1.0, 2.0, 3.0, 4.0, 2.0,
            3.0, 4.0, 4.0, 4.0, 3.0,
            2.0, 3.0, 2.0, 1.0, 1.0
           ],
    %% numpy.std(Data) computes it to:
    Expected = 1.0832051206181281,
    %% There is a possibility of Erlang computing it differently, on
    %% fairy 16-bit architectures or some such. If this happens, we
    %% need to run python on that arch to figure out what Expected
    %% value can be then.  Or, introduce an epsilon and check that the
    %% delta is small enough.
    State9 = lists:foldl(fun 'STDDEV_POP'/2, State0, Data),
    Got = finalise('STDDEV_POP', State9),
    ?assertEqual(Expected, Got).

stddev_samp_test() ->
    State0 = start_state('STDDEV_SAMP'),
    Data = [
            1.0, 2.0, 3.0, 4.0, 2.0,
            3.0, 4.0, 4.0, 4.0, 3.0,
            2.0, 3.0, 2.0, 1.0, 1.0
           ],
    State9 = lists:foldl(fun 'STDDEV_SAMP'/2, State0, Data),
    %% expected value calulated usingpostgres STDDEV_SAMP
    ?assertEqual(
        1.1212238211627762,
        finalise('STDDEV_SAMP', State9)
    ).

stddev_pop_no_value_test() ->
    ?assertEqual(
        [], 
        finalise('STDDEV_POP', start_state('STDDEV_POP'))
    ).
stddev_pop_one_value_test() ->
    ?assertEqual(
        ?SQL_NULL, 
        finalise('STDDEV_POP', 'STDDEV_POP'(3, start_state('STDDEV_POP')))
    ).
stddev_pop_two_value_test() ->
    ?assertEqual(
        0.5, 
        finalise('STDDEV_POP', lists:foldl(fun 'STDDEV_POP'/2, start_state('STDDEV_POP'), [1.0,2.0]))
    ).


testing_fold_avg(InitialState, InputList) ->
    finalise('AVG', lists:foldl(fun 'AVG'/2, InitialState, InputList)).

avg_integer_test() ->
    ?assertEqual(
        10 / 4, 
        testing_fold_avg(start_state('AVG'), [1,2,3,4])
    ).
avg_double_test() ->
    ?assertEqual(
        10 / 4, 
        testing_fold_avg(start_state('AVG'), [1.0,2.0,3.0,4.0])
    ).
avg_null_right_test() ->
    ?assertEqual(1.0, finalise('AVG', 'AVG'(1, ?SQL_NULL))).
avg_null_null_test() ->
    ?assertEqual(?SQL_NULL, 'AVG'(?SQL_NULL, ?SQL_NULL)).
avg_finalise_null_test() ->
    ?assertEqual(?SQL_NULL, finalise('AVG', start_state('AVG'))).

sum_null_state_arg_integer_test() ->
    ?assertEqual(1, 'SUM'(1, ?SQL_NULL)).
sum_integer_state_arg_integer_test() ->
    ?assertEqual(4, 'SUM'(1, 3)).
sum_double_state_arg_double_test() ->
    ?assertEqual(4.5, 'SUM'(1.2, 3.3)).
sum_null_state_arg_double_test() ->
    ?assertEqual(1.1, 'SUM'(1.1, ?SQL_NULL)).
sum_null_null_test() ->
    ?assertEqual(?SQL_NULL, 'SUM'(?SQL_NULL, ?SQL_NULL)).
sum_finalise_null_test() ->
    ?assertEqual(?SQL_NULL, finalise('SUM', start_state('SUM'))).

min_integer_test() ->
    ?assertEqual(erlang:min(1, 3), 'MIN'(1, 3)).
min_double_test() ->
    ?assertEqual(erlang:min(1.0, 3.0), 'MIN'(1.0, 3.0)).
min_null_left_test() ->
    ?assertEqual(3, 'MIN'(?SQL_NULL, 3)).
min_null_right_test() ->
    ?assertEqual(1, 'MIN'(1, ?SQL_NULL)).
min_null_null_test() ->
    ?assertEqual(?SQL_NULL, 'MIN'(?SQL_NULL, ?SQL_NULL)).
min_finalise_null_test() ->
    ?assertEqual(?SQL_NULL, finalise('MIN', start_state('MIN'))).

max_test() ->
    ?assertEqual('MAX'(1, 3), erlang:max(1, 3)).
max_double_test() ->
    ?assertEqual(erlang:max(1.0, 3.0), 'MAX'(1.0, 3.0)).
max_null_left_test() ->
    ?assertEqual(3, 'MAX'(?SQL_NULL, 3)).
max_null_right_test() ->
    ?assertEqual(1, 'MAX'(1, ?SQL_NULL)).
max_null_null_test() ->
    ?assertEqual(?SQL_NULL, 'MAX'(?SQL_NULL, ?SQL_NULL)).
max_finalise_null_test() ->
    ?assertEqual(?SQL_NULL, finalise('MAX', start_state('MAX'))).

testing_fold_agg(FnName, InitialState, InputList) ->
    finalise(FnName, lists:foldl(
        fun(E, Acc) ->
            ?MODULE:FnName(E, Acc) 
        end, InitialState, InputList)).

count_no_values_test() ->
    ?assertEqual(0, finalise('COUNT', start_state('COUNT'))).
count_all_null_values_test() ->
    ?assertEqual(
        0,
        testing_fold_agg('COUNT', start_state('COUNT'), [?SQL_NULL, ?SQL_NULL])
    ).
count_some_null_values_test() ->
    ?assertEqual(
        2, 
        testing_fold_agg('COUNT', start_state('COUNT'), [?SQL_NULL, <<"bob">>, ?SQL_NULL, <<"boris">>])
    ).
count_values_test() ->
    ?assertEqual(
        4, 
        testing_fold_agg('COUNT', start_state('COUNT'), [1,2,3,4])
    ).
count_rows_test() ->
    ?assertEqual(
        4, 
        testing_fold_agg('COUNT', start_state('COUNT'), [[1,2,3,4],[1,2,3,4],[1,2,3,4],[1,2,3,4]])
    ).
count_rows_with_nulls_test() ->
    ?assertEqual(
        3, 
        testing_fold_agg('COUNT', start_state('COUNT'), [[1,2,3,4],?SQL_NULL,[1,2,3,4],[1,2,3,4]])
    ).
-endif.
