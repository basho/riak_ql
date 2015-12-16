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

-export([
         get_type_sig/1,
         get_starting_state/2,
         finalise/2,
         'COUNT'/2, 'SUM'/2, 'AVG'/2, 'MIN'/2, 'MAX'/2, 'STDEV'/2, 'MODE'/2, 'MEDIAN'/2
        ]).

-type group_function() :: 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'STDEV' |
                          %% and a few more involving more than a simple traversal
                          'MODE' | 'MEDIAN'.

-include("riak_ql_ddl.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").  %% for UINT_MAX etc

%% functions used in expression type validation
%% get_type_sig('+')   -> [
%%                         {sint64, sint64},
%%                         {double, double}
%%                        ];
%% get_type_sig('-')   -> [
%%                         {sint64, sint64},
%%                         {double, double}
%%                        ];
%% get_type_sig('/')   -> [
%%                         {sint64, sint64},
%%                         {double, double}
%%                        ];
%% get_type_sig('*')   -> [
%%                         {sint64, sint64},
%%                         {double, double}
%%                        ];
get_type_sig('COUNT' ) -> [{sint64, sint64}, {double, sint64}];
get_type_sig('SUM'   ) -> [{sint64, sint64}, {double, double}];
get_type_sig('MIN'   ) -> [{sint64, sint64}, {double, double}];
get_type_sig('MAX'   ) -> [{sint64, sint64}, {double, double}];
get_type_sig('MODE'  ) -> [{sint64, sint64}, {double, double}];
get_type_sig('MEDIAN') -> [{sint64, sint64}, {double, double}];
get_type_sig('AVG'   ) -> [{sint64, double}, {double, double}];  %% double promotion
get_type_sig('STDEV' ) -> [{sint64, double}, {double, double}].  %% ditto

%% '+'(A, B) -> A + B.
%% '-'(A, B) -> A - B.
%% '/'(A, B ) -> A / B.
%% '*'(A, B) -> A * B.

-spec get_starting_state(field_type(), group_function()) ->
                                tuple() | number().
get_starting_state(_, 'COUNT') ->
    0;
get_starting_state(Type, Fun)
  when Fun == 'SUM';
       Fun == 'AVG';
       Fun == 'MODE';
       Fun == 'MEDIAN' ->
    zero_of_type_with_counter(Type);
get_starting_state(_Type, Fun)
  when Fun == 'AVG' ->
    zero_of_type_with_counter(double);
get_starting_state(_, Fun)
  when Fun == 'MIN';
       Fun == 'MAX' ->
    not_a_value;
get_starting_state(_, 'STDEV') ->
    {0, 0.0, 0.0}.
zero_of_type_with_counter(sint64) -> {0, 0};
zero_of_type_with_counter(double) -> {0, 0.0}.


%% Group functions (avg, mean etc). These can only appear as top-level
%% expressions in SELECT part, and there can be only one in a query.
%% Can take an Expr that includes the column identifier and some static
%% values.
%%
%% Incrementally operates on chunks, needs to carry state.


'COUNT'(_Arg, _State = N) ->
    N + 1.

'SUM'  (Arg, _State = Total) ->
    Arg + Total.

'AVG'  (Arg, _State = {N, Acc}) ->
    {N + 1, Acc + Arg}.

'MIN'  (Arg, _State = Min) ->
    if Min == not_a_value ->
            Min;
       Arg < Min ->
            Arg;
       el/=se ->
            Min
    end.

'MAX'  (Arg, _State = Max) ->
    if Max == not_a_value ->
            Max;
       Arg > Max ->
            Arg;
       el/=se ->
            Max
    end.

'MODE'  (_Arg, _State) ->
    %% stub
    _State.

'MEDIAN'(_Arg, _State) ->
    %% stub
    _State.

'STDEV'(Arg, _State = {N, SumOfSquares, Mean}) ->
    {N + 1, SumOfSquares + Arg * Arg, Mean + (Mean - Arg) / (N+1)}.


finalise('COUNT', N) ->
    N;
finalise('SUM', Sum) ->
    Sum;
finalise('AVG', {N, Acc}) ->
    Acc / N;
finalise('MIN', Acc) ->
    Acc;
finalise('MAX', Acc) ->
    Acc;
finalise('MODE', Acc) ->
    Acc;
finalise('MEDIAN', Acc) ->
    Acc;
finalise('STDEV', {N, SumOfSquares, Mean}) ->
    %% TODO: rework using special formulae to prevent overflows
    math:sqrt(SumOfSquares / (N - Mean * Mean)).
