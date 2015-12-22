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


-export(['COUNT'/2, 'SUM'/2, 'AVG'/2, 'MEAN'/2, 'MIN'/2, 'MAX'/2, 'STDEV'/2]).
-export([finalise/2]).
-export([start_state/1]).
-export([get_arity_and_type_sig/1]).


-type aggregate_function() :: 'COUNT' | 'SUM' | 'AVG' |'MEAN' | 'MIN' | 'MAX' | 'STDEV'.

-include("riak_ql_ddl.hrl").

%% functions used in expression type validation
get_arity_and_type_sig('AVG')    -> {1, [{sint64, double}, {double, double}]};  %% double promotion
get_arity_and_type_sig('MEAN')   -> {1, [{sint64, double}, {double, double}]};  %% double promotion
get_arity_and_type_sig('COUNT')  -> {1, [{sint64, sint64}, {double, sint64}]};
get_arity_and_type_sig('MAX')    -> {1, [{sint64, sint64}, {double, double}]};
get_arity_and_type_sig('MIN')    -> {1, [{sint64, sint64}, {double, double}]};
get_arity_and_type_sig('STDEV')  -> {1, [{sint64, double}, {double, double}]};  %% ditto
get_arity_and_type_sig('SUM')    -> {1, [{sint64, sint64}, {double, double}]}.

%% Get the initial accumulator state for the aggregation.
-spec start_state(aggregate_function()) ->
    any().
start_state('AVG') -> 0;
start_state('MEAN') -> 0;
start_state('COUNT') -> 0;
start_state('MAX') -> not_a_value;
start_state('MIN') -> not_a_value;
start_state('STDEV') -> {0, 0.0, 0.0};
start_state('SUM') -> 0;
start_state(_) -> stateless.

%% Calculate the final results using the accumulated result.
-spec finalise(aggregate_function(), any()) -> any().
finalise('MEAN', {N, Acc}) ->
    finalise('AVG', {N, Acc});
finalise('AVG', {N, Acc}) ->
    Acc / N;
finalise('STDEV', {N, SumOfSquares, Mean}) ->
    %% TODO: rework using special formulae to prevent overflows
    math:sqrt(SumOfSquares / (N - Mean * Mean));
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

'MEAN'(Arg, State) ->
    'AVG'(Arg, State).

'AVG'(Arg, _State = {N, Acc}) ->
    {N + 1, Acc + Arg}.

'MIN'(Arg, not_a_value)        -> Arg;
'MIN'(Arg, Acc) when Arg < Acc -> Arg;
'MIN'(Arg, _)                  -> Arg.

'MAX'(Arg, not_a_value)        -> Arg;
'MAX'(Arg, Acc) when Arg > Acc -> Arg;
'MAX'(Arg, _)                  -> Arg.

'STDEV'(Arg, _State = {N, SumOfSquares, Mean}) ->
    {N + 1, SumOfSquares + Arg * Arg, Mean + (Mean - Arg) / (N+1)}.
