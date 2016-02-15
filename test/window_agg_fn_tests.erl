%% -------------------------------------------------------------------
%%
%% Window Aggregatation Tests
%%
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

-module(window_agg_fn_tests).

-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

% %%
% %% A coupla chunks of yer actual data
% %%
% -define(MATRIX1, [
%                   [{<<"pooter1">>,<<"ZXC11">>},
%                    {<<"myvarchar">>,<<"PDP-11">>},
%                   {<<"mytimestamp">>,10000011},
%                   {<<"myboolean">>, true}, 
%                   {<<"mysint64">>, 1}, 
%                   {<<"mydouble">>,1.0}],
%                  [{<<"pooter1">>,<<"ZXC11">>},
%                   {<<"myvarchar">>,<<"PDP-11">>},
%                   {<<"mytimestamp">>,10000012},
%                   {<<"myboolean">>, true}, 
%                   {<<"mysint64">>, 2}, 
%                   {<<"mydouble">>,2.0}],
%                  [{<<"pooter1">>,<<"ZXC11">>},
%                   {<<"myvarchar">>,<<"PDP-11">>},
%                   {<<"mytimestamp">>,10000013},
%                   {<<"myboolean">>, true}, 
%                   {<<"mysint64">>, 3}, 
%                   {<<"mydouble">>,4.0}],
%                  [{<<"pooter1">>,<<"ZXC11">>},
%                   {<<"myvarchar">>,<<"PDP-11">>},
%                   {<<"mytimestamp">>,10000014},
%                   {<<"myboolean">>, true}, 
%                   {<<"mysint64">>, 4}, 
%                   {<<"mydouble">>,4.0}],
%                   [{<<"pooter1">>,<<"ZXC11">>},
%                   {<<"myvarchar">>,<<"PDP-11">>},
%                   {<<"mytimestamp">>,10000015},
%                   {<<"myboolean">>, true}, 
%                   {<<"mysint64">>, 5},
%                   {<<"mydouble">>,5.0}]
%                  ]).

% -define(MATRIX2, [
%                  [{<<"pooter1">>,<<"ZXC11">>},
%                   {<<"myvarchar">>,<<"PDP-11">>},
%                   {<<"mytimestamp">>,10000016},
%                   {<<"myboolean">>, false}, 
%                   {<<"mysint64">>, 6},
%                   {<<"mydouble">>,6.0}],
%                  [{<<"pooter1">>,<<"ZXC11">>},
%                   {<<"myvarchar">>,<<"PDP-11">>},
%                   {<<"mytimestamp">>,10000017},
%                   {<<"myboolean">>, false}, 
%                   {<<"mysint64">>, 7},
%                   {<<"mydouble">>,7.0}],
%                  [{<<"pooter1">>,<<"ZXC11">>},
%                   {<<"myvarchar">>,<<"PDP-11">>},
%                   {<<"mytimestamp">>,10000018},
%                   {<<"myboolean">>, false}, 
%                   {<<"mysint64">>, 8},
%                   {<<"mydouble">>,8.0}],
%                  [{<<"pooter1">>,<<"ZXC11">>},
%                   {<<"myvarchar">>,<<"PDP-11">>},
%                   {<<"mytimestamp">>,10000019},
%                   {<<"myboolean">>, false}, 
%                   {<<"mysint64">>, 9},
%                   {<<"mydouble">>,9.0}]
%                  ]).

% make_chunk() ->
%     Bucket = <<"FakeBucket">>,
%     Key = <<"FakeKey">>,
%     EncodeFn = fun(V) ->
%                        Obj = riak_object:new(Bucket, Key, V),
%                        riak_object:to_binary(v1, Obj, msgpack)
%                end,
%     Matrix = ?MATRIX1 ++ ?MATRIX2,
%     Objs = [{Key, EncodeFn(X)} || X <- Matrix],
%     Matrix2 = dump(Objs),
%     case Matrix2 of
%         Matrix -> ok;
%         _Err   -> io:format("Matrix is: -~p~nReversed is:~n- ~p~n", [Matrix, Matrix2]),
%                   exit(duffo)
%     end.

% dump(Data) ->
%     ExtractFn = fun(V) ->
%                         RObj = riak_object:from_binary(<<>>, <<>>, V),
%                         Rec = riak_object:get_value(RObj),
%                         gg:format("V is ~p~nRObj is ~p~nRec is ~p~n",
%                                   [V, RObj, Rec]),
%                         Rec
%                 end,
%     %% gg:format("Data is ~p~n", [Data]),
%     Raw = [ExtractFn(V) || {_K, V} <- Data],
%     %% gg:format("Raw is ~p~n", [Raw]),
%     Raw.

% %%
% %% Test Helpers
% %%
% select(_Data, _SQL, _Expected) ->
%     ?assertEqual(bish, bash).

% select_fail(_Data, _SQL, _Expected) ->
%     ?assertEqual(fish, fash).

% %%
% %% Unit tests
% %%
% simple_select_test() ->
%     Data = [?MATRIX1],
%     SQL = "select time from blah",
%     Expected = [
%                  [10000011],
%                  [10000012],
%                  [10000013],
%                  [10000014],
%                  [10000015]
%                ],
%     select(Data, SQL, Expected).

% simple_select_arith_test() ->
%     Data = [?MATRIX1],
%     SQL = "select time + 10000000, time + 10000000.0, from blah",
%     Expected = [
%                  [20000011, 20000011.0],
%                  [20000012, 20000012.0],
%                  [20000013, 20000013.0],
%                  [20000014, 20000014.0],
%                  [20000015, 20000015.0]
%                ],
%     select(Data, SQL, Expected).

% simple_select_avg_1_test() ->
%     Data = [?MATRIX1],
%     SQL = "select avg(mysint64), avg(mydouble), avg(mytimestamp) from blah",
%     Expected = [
%                 [3.0, 3.0, 10000013.0]
%                ],
%     select(Data, SQL, Expected).

% select_count_with_wildcards_test() ->
%     Data = [?MATRIX1],
%     SQL = "select count(*), count(mydouble) from blah",
%     Expected = [
%                 [5, 5]
%                ],
%     select(Data, SQL, Expected).

% simple_select_all_fns_1_test() ->
%     Data = [?MATRIX1],
%     SQL = "select " ++
%         "avg(mysint64),   avg(mydouble),   avg(mytimestamp), " ++ 
%         "mean(mysint64),  mean(mydouble),  mean(mytimestamp), " ++ 
%         "count(mysint64), count(mydouble), count(mytimestamp), " ++ 
%         "max(mysint64),   max(mydouble),   max(mytimestamp), " ++ 
%         "min(mysint64),   min(mydouble),   min(mytimestamp), " ++ 
%         "from blah",
%     Expected = [
%                 [
%                  3.0, 3.0, 10000013.0, %% avg
%                  3.0, 3.0, 10000013.0, %% mean
%                  5,   5,   5,          %% count
%                  5,   5.0, 10000015,   %% max
%                  1,   1.0, 10000015    %% min
%                 ]
%                ],
%     select(Data, SQL, Expected).
    
% complex_select_avg_1_test() ->
%     Data = [?MATRIX1],
%     SQL = "select avg(mysint64 + 2) from blah",
%     Expected = [
%                 [3.0]
%                ],
%     select(Data, SQL, Expected).

% chunked_select_all_fns_1_test() ->
%     Data = [?MATRIX1 ++ ?MATRIX2],
%     SQL = "select " ++
%         "avg(mysint64),   avg(mydouble),   avg(mytimestamp), " ++ 
%         "mean(mysint64),  mean(mydouble),  mean(mytimestamp), " ++ 
%         "count(mysint64), count(mydouble), count(mytimestamp), " ++ 
%         "max(mysint64),   max(mydouble),   max(mytimestamp), " ++ 
%         "min(mysint64),   min(mydouble),   min(mytimestamp), " ++ 
%         "from blah",
%     Expected = [
%                 [
%                  5.0, 5.0, 10000015.0, %% avg
%                  5.0, 5.0, 10000015.0, %% mean
%                  9,   9,   9,          %% count
%                  9,   9.0, 10000019,   %% max
%                  1,   1.0, 10000015    %% min
%                 ]
%                ],
%     select(Data, SQL, Expected).

% %%
% %% failing tests
% %%

% simple_select_avg_fail_1_test() ->
%     Data = [?MATRIX1],
%     SQL = "select avg(myboolean) from blah",
%     Expected = "some error message",
%     select_fail(Data, SQL, Expected).

% simple_select_avg_fail_2_test() ->
%     Data = [?MATRIX1],
%     SQL = "select avg(myvar) from blah",
%     Expected = "some error message",
%     select_fail(Data, SQL, Expected).
    
