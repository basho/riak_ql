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


-export(['PERCENTILE_DISC'/3,
         'PERCENTILE_CONT'/3,
         'MEDIAN'/3,
         'MODE'/3]).
-export([fn_arity/1,
         fn_type_signature/2,
         fn_param_check/2,
         supported_functions/0]).

-type invdist_function() :: 'PERCENTILE_CONT'
                          | 'PERCENTILE_DISC'
                          | 'MEDIAN'
                          | 'MODE'.
-export_type([invdist_function/0]).

-include("riak_ql_ddl.hrl").

supported_functions() ->
    ['PERCENTILE_DISC', 'PERCENTILE_CONT', 'MEDIAN', 'MODE'].

-spec fn_type_signature(invdist_function(), [riak_ql_ddl:external_field_type()]) ->
                               riak_ql_ddl:external_field_type() |
                               {error, term()}.
fn_type_signature('PERCENTILE_DISC', [ColumnType, double])
  when ColumnType == sint64;
       ColumnType == double;
       ColumnType == timestamp -> ColumnType;
fn_type_signature('PERCENTILE_CONT', [ColumnType, double])
  when ColumnType == sint64;
       ColumnType == double;
       ColumnType == timestamp -> double;
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
fn_arity('PERCENTILE_CONT') -> 2;
fn_arity('PERCENTILE_DISC') -> 2;
fn_arity('MEDIAN') -> 1;
fn_arity('MODE') -> 1;
fn_arity(_) -> {error, invalid_function}.

-spec fn_param_check(invdist_function(), [riak_ql_ddl:external_field_type()]) ->
                            ok | {error, WhichParamInvalid::pos_integer()}.
fn_param_check(PcntlFn, [Pc])
  when (PcntlFn == 'PERCENTILE_CONT' orelse PcntlFn == 'PERCENTILE_DISC') andalso
       (Pc >= 0.0 andalso Pc =< 1.0) ->
    ok;
fn_param_check(PcntlFn, [_Pc])
  when (PcntlFn == 'PERCENTILE_CONT' orelse PcntlFn == 'PERCENTILE_DISC') ->
    {error, 2};
fn_param_check('MEDIAN', []) ->
    ok;
fn_param_check('MODE', []) ->
    ok.


%% functions defined
%%
%% Note that ValuesAtF expects row position to be 0-based.

'PERCENTILE_DISC'([Pc], RowsTotal, ValuesAtF) ->
    RN = (Pc * (RowsTotal - 1)),
    [[Ret]] = ValuesAtF([{trunc(RN), 1}]),
    Ret.

'PERCENTILE_CONT'([Pc], RowsTotal, ValuesAtF) ->
    RN = (Pc * (RowsTotal - 1)),
    {LoRN, HiRN} = {trunc(RN), ceil(RN)},
    case LoRN == HiRN of
        true ->
            [[Val]] = ValuesAtF([{LoRN, 1}]),
            Val;
        false ->
            [[LoVal], [HiVal]] = ValuesAtF([{LoRN, 1}, {HiRN, 1}]),
            (HiRN - RN) * LoVal + (RN - LoRN) * HiVal
    end.

'MEDIAN'([], RowsTotal, ValuesAtF) ->
    'PERCENTILE_DISC'([0.5], RowsTotal, ValuesAtF).

'MODE'([], RowsTotal, ValuesAtF) ->
    [[Min]] = ValuesAtF([{0, 1}]),
    largest_bin(Min, ValuesAtF, RowsTotal).

%% This will be inefficient for ldb backends (that is, when a qbuf is
%% dumped to leveldb): in this function, we call ValuesAtF to retrieve
%% one row at a time.  This means, each time it is called,
%% `riak_kv_qry_buffers_ldb:fetch_rows` needs to seek from start and
%% trundle all the way to the Nth position, and all over again to
%% fetch N+1th row.  The obvious todo item it to either teach
%% fetch_rows to cache iterators or, alternatively, fetch rows in
%% chunks ourselves.
largest_bin(Min, ValuesAtF, RowsTotal) ->
    largest_bin_({Min, 1, Min, 1}, ValuesAtF, 1, RowsTotal).

largest_bin_({LargestV, _, _, _}, _ValuesAtF, Pos, RowsTotal) when Pos >= RowsTotal ->
    LargestV;
largest_bin_({LargestV, LargestC, CurrentV, CurrentC}, ValuesAtF, Pos, RowsTotal) ->
    case ValuesAtF([{Pos, 1}]) of
        [[V]] when V == CurrentV ->
            largest_bin_({LargestV, LargestC,  %% collecting current bin
                          CurrentV, CurrentC + 1}, ValuesAtF, Pos + 1, RowsTotal);
        [[V]] when V > CurrentV,
                   CurrentC > LargestC ->
            largest_bin_({CurrentV, CurrentC,  %% now these be largest
                          V, 1}, ValuesAtF, Pos + 1, RowsTotal);
        [[V]] when V > CurrentV,
                   CurrentC =< LargestC ->
            largest_bin_({LargestV, LargestC,  %% keep largest, reset current
                          V, 1}, ValuesAtF, Pos + 1, RowsTotal)
    end.

ceil(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.
