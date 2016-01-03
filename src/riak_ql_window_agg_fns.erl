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
get_arity_and_type_sig('COUNT')  -> {1, [{sint64, sint64}, {double, sint64}, {boolean, sint64}, {varchar, sint64}, {timestamp, sint64}]};
get_arity_and_type_sig('AVG')    -> {1, [{sint64, double}, {double, double}]};  %% double promotion
get_arity_and_type_sig('MEAN')   -> get_arity_and_type_sig('AVG'); 
get_arity_and_type_sig('MAX')    -> {1, [{sint64, sint64}, {double, double}]};
get_arity_and_type_sig('MIN')    -> {1, [{sint64, sint64}, {double, double}]};
get_arity_and_type_sig('STDEV')  -> {1, [{sint64, double}, {double, double}]};  %% ditto
get_arity_and_type_sig('SUM')    -> {1, [{sint64, sint64}, {double, double}]}.

%% Get the initial accumulator state for the aggregation.
-spec start_state(aggregate_function()) ->
    any().
start_state('AVG')   -> {0, 0.0};
start_state('MEAN')  -> start_state('AVG');
start_state('COUNT') -> 0;
start_state('MAX')   -> not_a_value;
start_state('MIN')   -> not_a_value;
start_state('STDEV') -> {0, 0.0, 0.0};
start_state('SUM')   -> 0;
start_state(_)       -> stateless.

%% Calculate the final results using the accumulated result.
-spec finalise(aggregate_function(), any()) -> any().
finalise('MEAN', {N, Acc}) ->
    finalise('AVG', {N, Acc});
%% TODO
finalise('AVG', {0, _}) ->
    exit('need to handle nulls boyo');
finalise('AVG', {N, Acc}) ->
    Acc / N;
finalise('STDEV', {N, _A, Q}) ->
    math:sqrt(Q / N);
finalise(_, not_a_value) ->
    exit('still need to handle nulls boyo');
finalise(_Fn, Acc) ->
    Acc.

%% Group functions (avg, mean etc). These can only appear as top-level
%% expressions in SELECT part, and there can be only one in a query.
%% Can take an Expr that includes the column identifier and some static
%% values.
%%
%% Incrementally operates on chunks, needs to carry state.


'COUNT'(_, N) when is_integer(N) ->
    N + 1.

'SUM'({_ColName, Arg}, _State = Total) when is_number(Arg) ->
    Arg + Total;
'SUM'({_ColName, _Arg}, State) ->
    State.

'MEAN'(Arg, State) ->
    'AVG'(Arg, State).

'AVG'({_ColName, Arg}, _State = {N, Acc}) when is_number(Arg) ->
    {N + 1, Acc + Arg};
'AVG'(_Arg, State) ->
    State.

'MIN'({_ColName, []},   State)                  -> State;
'MIN'({_ColName, Arg},  not_a_value)            -> Arg;
'MIN'({_ColName, Arg},  State) when Arg < State -> Arg;
'MIN'({_ColName, _Arg}, State)                  -> State.

'MAX'({_ColName, []},   State)                  -> State;
'MAX'({_ColName, Arg},  not_a_value)            -> Arg;
'MAX'({_ColName, Arg},  State) when Arg > State -> Arg;
'MAX'({_ColName, _Arg}, State)                  -> State.

'STDEV'({_ColName, Arg}, _State = {N_old, A_old, Q_old}) when is_number(Arg) ->
    %% A and Q are those in https://en.wikipedia.org/wiki/Standard_deviation#Rapid_calculation_methods
    N = N_old + 1,
    A = A_old + (Arg - A_old) / N,
    Q = Q_old + (Arg - A_old) * (Arg - A),
    {N, A, Q};
'STDEV'(_, State) ->
    State.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(COL, <<"fake column name">>).

stdev_test() ->
    State0 = start_state('STDEV'),
    Data = [
            {?COL, 1.0}, {?COL, 2.0}, {?COL, 3.0}, {?COL, 4.0}, {?COL, 2.0}, 
            {?COL, 3.0}, {?COL, 4.0}, {?COL, 4.0}, {?COL, 4.0}, {?COL, 3.0}, 
            {?COL, 2.0}, {?COL, 3.0}, {?COL, 2.0}, {?COL, 1.0}, {?COL, 1.0}
           ],
    %% numpy.std(Data) computes it to:
    Expected = 1.0832051206181281,
    %% There is a possibility of Erlang computing it differently, on
    %% fairy 16-bit architectures or some such. If this happens, we
    %% need to run python on that arch to figure out what Expected
    %% value can be then.  Or, introduce an epsilon and check that the
    %% delta is small enough.
    State9 = lists:foldl(fun 'STDEV'/2, State0, Data),
    Got = finalise('STDEV', State9),
    ?assertEqual(Expected, Got).

-endif.
