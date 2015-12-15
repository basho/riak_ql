%% -------------------------------------------------------------------
%%
%% riak_ql_window_agg_fns: implementation of Windows Aggregation Fns
%%                         for the query runner
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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
         get_starting_acc/1,
         finalise/2,
         '+'/2,
         '-'/2,
         '/'/2,
         '*'/2,
         'AVG'/3
         ]).

get_type_sig('+')   -> [
                        {sint64, sint64},
                        {double, double}
                       ];
get_type_sig('-')   -> [
                        {sint64, sint64},
                        {double, double}
                       ];
get_type_sig('/')   -> [
                        {sint64, double},
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

get_starting_acc('AVG') -> {0, 0}.

finalise('AVG', {Total, No}) -> Total/No.

'AVG'(RObjs, Expr, Acc) when is_list(RObjs) ->
    FoldFun = fun(Rec, {Total, No}) when is_list(Rec) ->
                      V = eval(Expr, Rec),
                      {Total + V, No + 1}
              end,
    lists:foldl(FoldFun, Acc, RObjs).

eval({identifier, ColName}, Rec) ->
    {ColName, V} = lists:keyfind(ColName, 1, Rec),
    V;
eval(Expr, RObj) ->
    io:format("Expr is ~p~n RObj is ~p~n", [Expr, RObj]),
    12345.
