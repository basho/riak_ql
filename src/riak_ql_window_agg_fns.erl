%% -------------------------------------------------------------------
%%
%% riak_ql_window_agg_fns: implementation of Windows Aggregation Fns
%%                         for the query runner
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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


-export(['COUNT'/2, 'SUM'/2, 'AVG'/2, 'MIN'/2, 'MAX'/2, 'STDEV'/2]).
-export([finalise/2]).
-export([start_state/1]).
-export([get_type_sig/1]).


-type aggregate_function() :: 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'STDEV'.

-include("riak_ql_ddl.hrl").

%% functions used in expression type validation
get_type_sig('AVG')    -> [{sint64, double}, {double, double}];  %% double promotion
get_type_sig('COUNT')  -> [{sint64, sint64}, {double, sint64}];
get_type_sig('MAX')    -> [{sint64, sint64}, {double, double}];
get_type_sig('MIN')    -> [{sint64, sint64}, {double, double}];
get_type_sig('STDEV')  -> [{sint64, double}, {double, double}];  %% ditto
get_type_sig('SUM')    -> [{sint64, sint64}, {double, double}].

%% Get the initial accumulator state for the aggregation.
-spec start_state(aggregate_function()) ->
    any().
start_state('AVG') -> 0;
start_state('COUNT') -> 0;
start_state('MAX') -> not_a_value;
start_state('MIN') -> not_a_value;
start_state('STDEV') -> {0, 0.0, 0.0};
start_state('SUM') -> 0;
start_state(_) -> stateless.

%% Calculate the final results using the accumulated result.
-spec finalise(aggregate_function(), any()) -> any().
finalise('AVG', {N, Acc}) ->
    Acc / N;
finalise('STDEV', {N, _A, Q}) ->
    math:sqrt(Q / N);
finalise(_, not_a_value) ->
    0;
finalise(_, Acc) ->
    Acc.

%% Group functions (avg, mean etc). These can only appear as top-level
%% expressions in SELECT part, and there can be only one in a query.
%% Can take an Expr that includes the column identifier and some static
%% values.
%%
%% Incrementally operates on chunks, needs to carry state.


'COUNT'(_, N) when is_integer(N) ->
    N + 1.

'SUM'(Arg, _State = Total) ->
    Arg + Total.

'AVG'(Arg, _State = {N, Acc}) ->
    {N + 1, Acc + Arg}.

'MIN'(Arg, not_a_value)        -> Arg;
'MIN'(Arg, Acc) when Arg < Acc -> Arg;
'MIN'(Arg, _)                  -> Arg.

'MAX'(Arg, not_a_value)        -> Arg;
'MAX'(Arg, Acc) when Arg > Acc -> Arg;
'MAX'(Arg, _)                  -> Arg.

'STDEV'(Arg, _State = {N_, A_, Q_}) ->
    %% A and Q are those in https://en.wikipedia.org/wiki/Standard_deviation#Rapid_calculation_methods
    N = N_ + 1,
    A = A_ + (Arg - A_) / N,
    Q = Q_ + (Arg - A_) * (Arg - A),
    {N, A, Q}.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

stdev_test() ->
    State0 = start_state('STDEV'),
    Data = [1.0, 2.0, 3.0, 4.0, 2.0, 3.0, 4.0, 4.0, 4.0, 3.0, 2.0, 3.0, 2.0, 1.0, 1.0],
    %% numpy.std(Data) computes it to:
    Expected = 1.0832051206181281,
    State9 = lists:foldl(fun(X, State) -> 'STDEV'(X, State) end, State0, Data),
    Got = finalise('STDEV', State9),
    ?assertEqual(Expected, Got).

-endif.
