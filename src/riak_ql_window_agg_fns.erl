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
         group_fn_starting_acc/2,
         group_fn_start/3,
         group_fn_continue/3,
         group_fn_finalise/2,
         '+'/2,
         '-'/2,
         '/'/2,
         '*'/2,
         'AVG'/3
         ]).

-type operator() :: '+' | '-' | '*' | '/'.
-type group_function() :: 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'STDEV' |
                          %% and a few more involving more than a simple traversal
                          'MODE' | 'MEDIAN'.

-include("riak_ql_ddl.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").  %% for UINT_MAX etc

%% functions used in expression type validation
get_type_sig('+')   -> [
                        {sint64, sint64},
                        {double, double}
                       ];
get_type_sig('-')   -> [
                        {sint64, sint64},
                        {double, double}
                       ];
get_type_sig('/')   -> [
                        {sint64, sint64},
                        {double, double}
                       ];
get_type_sig('*')   -> [
                        {sint64, sint64},
                        {double, double}
                       ];
get_type_sig('AVG') -> [
                        {sint64, double},
                        {double, double}
                       ].

'+'(A, B) -> A + B.

'-'(A, B) -> A - B.

'/'(A, B ) -> A / B.

'*'(A, B) -> A * B.

-spec starting_state(field_type(), group_function()) ->
                            {StartingValue::number(), N::non_neg_integer()} |
                            number().
starting_state(_, 'COUNT') ->
    0;
starting_state(Type, Fun)
  when Fun == 'SUM';
       Fun == 'AVG';
       Fun == 'MODE';
       Fun == 'MEDIAN' ->
    zero_of_type(Type);
starting_state(_, Fun)
  when Fun == 'MIN';
       Fun == 'MAX' ->
    not_a_value;
starting_state(_, 'STDEV') ->
    {0, 0.0, 0.0}.



%% Group functions (avg, mean etc). These can only appear as top-level
%% expressions in SELECT part, and there can be only one in a query.
%% Can take an Expr that includes the column identifier and some static
%% values.
%%
%% Incrementally operates on chunks, needs to carry state.
group_fn_run('COUNT', _Type, _Expr, {identifier, _ColName}, _Rec, Acc) ->
    fun(_V, Acc) ->
            Acc + 1
    end;
group_fn_run() ->


group_fn_finish('AVG', _Type, {N, Acc}) ->
    Acc / N;
group_fn_finish('SUM', _Type, Acc) ->
    Acc;
group_fn_finish('MIN', _Type, Acc) ->
    Acc;
group_fn_finish('MAX', _Type, Acc) ->
    Acc;
group_fn_finish('STDEV', _Type, {N, SumOfSquares, Mean}) ->
    %% TODO: rework using special formulae to prevent overflows
    math:sqrt(SumOfSquares / N - Mean * Mean).


'AVG'(RObjs, Expr, Acc) when is_list(RObjs) ->
    FoldFun = fun(Rec, {Total, No}) when is_list(Rec) ->
                      V = eval(Expr, Rec),
                      {Total + V, No + 1}
              end,
    lists:foldl(FoldFun, Acc, RObjs).

eval({identifier, ColName}, Rec) ->
    {ColName, V} = lists:keyfind(ColName, 1, Rec),
    V;
