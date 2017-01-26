%% -------------------------------------------------------------------
%%
%% riak_ql_inverse_distrib_fns: implementation of inverse distribution functions
%%                              for the query runner
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_ql_inverse_distrib_fns).


-export(['PERCENTILE'/2, 'MEDIAN'/1, 'MODE'/1]).
-export([fn_arity/1,
         fn_type_signature/2,
         fn_param_check/2,
         supported_functions/0]).

-type invdist_function() :: 'PERCENTILE' | 'MEDIAN' | 'MODE'.
-export_type([invdist_function/0]).

-include("riak_ql_ddl.hrl").

supported_functions() ->
    ['PERCENTILE', 'MEDIAN', 'MODE'].

-spec fn_type_signature(invdist_function(), [riak_ql_ddl:external_field_type()]) ->
                               riak_ql_ddl:external_field_type() |
                               {error, term()}.
fn_type_signature('PERCENTILE', [ColumnType, double])
  when ColumnType == sint64;
       ColumnType == double;
       ColumnType == timestamp -> ColumnType;
fn_type_signature('MEDIAN', [ColumnType])
  when ColumnType == sint64;
       ColumnType == double;
       ColumnType == timestamp  -> ColumnType;
fn_type_signature('MODE', [ColumnType])
  when ColumnType == sint64;
       ColumnType == double;
       ColumnType == timestamp  -> ColumnType;
fn_type_signature(Fn, Args) ->
    {error, {argument_type_mismatch, Fn, Args}}.

-spec fn_arity(invdist_function()) -> non_neg_integer().
fn_arity('PERCENTILE') -> 2;
fn_arity('MEDIAN') -> 1;
fn_arity('MODE') -> 1;
fn_arity(_) -> {error, invalid_function}.

-spec fn_param_check(invdist_function(), [riak_ql_ddl:external_field_type()]) ->
                            ok | {error, WhichParamInvalid::pos_integer()}.
fn_param_check('PERCENTILE', [Pc])
  when Pc >= 0.0,
       Pc =< 1.0 ->
    ok;
fn_param_check('PERCENTILE', [_Pc]) ->
    {error, 2};
fn_param_check('MEDIAN', []) ->
    ok;
fn_param_check('MODE', []) ->
    ok.



'PERCENTILE'(RowsTotal, Pc) when 0.0 =< Pc, Pc =< 1.0 ->
    lists:min([1 + round(RowsTotal * Pc), RowsTotal]).

'MEDIAN'(RowsTotal) ->
    lists:min([1 + round(RowsTotal / 2), RowsTotal]).

'MODE'(_) ->
    erlang:error(dont_call_me).
