%% -------------------------------------------------------------------
%%
%% riak_ql_quanta.erl - a library for quantising time for Time Series
%%
%% @doc This module serves to generate time quanta on multi - (day, hour, minute,
%% second) boundaries. The quantum are based on an origin time of Jan 1, 1970 
%% 00:00:00 (Unix Epoch).
%% The function <em>quantum/3</em> takes a time in milliseconds to bucketize, 
%% a size of the quantum, and the units of said quantum.
%% For instance, the following call would create buckets for timestamps on 15
%% minute boundaries: <em>quantum(Time, 15, m)</em>. The quantum time is returned in 
%% milliseconds since the Unix epoch.
%% The function <em>quanta/4</em> takes 2 times in milliseconds and size of the quantum
%% and the of units of said quantum and returns a list of quantum boundaries that span the time
%%
%% Copyright (c) 2015-2016 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_ql_quanta).

-export([
         quantum/3,
         quanta/4,
         timestamp_to_ms/1,
         ms_to_timestamp/1,
         unit_to_millis/2
        ]).

-type time_ms() :: non_neg_integer().
%% A timestamp in millisconds representing number of millisconds from Unix epoch

-type time_unit() :: d | h | m | s | ms.
%%  The units of quantization available to quantum/3

-type err() :: {error, term()}.

%% @doc The Number of Days from Jan 1, 0 to Jan 1, 1970
%% We need this to compute years and months properly including leap years and variable length
%% months.
-define(DAYS_FROM_0_TO_1970, 719528).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-compile(export_all).
-endif.
-endif.
%% @clear
%% @end

%% @doc given an upper and lower bound for time, returns a tuple consisting of
%% * the number of slices
%% * a list of all the quantum boundaries
%%   - the length of the list is the number of slices - 1
-spec quanta(time_ms(), time_ms(), non_neg_integer(), time_unit()) -> {integer(), [integer()]} | {error, any()}.
quanta(StartTime, EndTime, QuantaSize, Unit) when StartTime > EndTime ->
    %% cheap trick to handle descending timestamps, reverse the arguments
    quanta(EndTime, StartTime, QuantaSize, Unit); 
quanta(StartTime, EndTime, QuantaSize, Unit) ->
    Start = quantum(StartTime, QuantaSize, Unit),
    case Start of
        {error, _} = E -> E;
        _Other         -> End = EndTime,
                          Diff = End - Start,
                          Slice = unit_to_ms(Unit) * QuantaSize,
                          NSlices = accommodate(Diff, Slice),
                          Quanta = gen_quanta(NSlices, Start, Slice, []),
                          {NSlices, Quanta}
    end.

%% compute ceil(Length / Unit)
accommodate(Length, Unit) ->
    Length div Unit + if Length rem Unit > 0 -> 1; el/=se -> 0 end.

gen_quanta(1, _Start, _Slice, Acc) ->
    Acc;
gen_quanta(N, Start, Slice, Acc) when is_integer(N) andalso N > 1 ->
    NewA = Start + (N - 1) * Slice,
    gen_quanta(N - 1, Start, Slice, [NewA | Acc]).

%% @doc Given the time in milliseconds since the unix epoch and a time range and unit eg (15, m),
%% generate the starting timestamp of the range (quantum) in milliseconds since the epoch where the
%% time belongs. Note that Time - Quanta is less than or equal to QuantaSize * Unit (in milliseconds).
-spec quantum(time_ms(), non_neg_integer(), time_unit()) -> time_ms() | err().
quantum(Time, QuantaSize, Unit) when Unit == d;
                                     Unit == h;
                                     Unit == m;
                                     Unit == s;
                                     Unit == ms ->
    Ms = unit_to_ms(Unit),
    Diff = Time rem (QuantaSize*Ms),
    Time - Diff;
quantum(_, _, Unit) ->
    {error, {invalid_unit, Unit}}.

%% Convert an integer and a time unit in binary to millis, assumed from the unix
%% epoch.
-spec unit_to_millis(Value::integer(), Unit::binary() | time_unit()) -> integer() | error.
unit_to_millis(V, U) when U == ms; U == <<"ms">> -> V;
unit_to_millis(V, U) when U == s; U == <<"s">> -> V*1000;
unit_to_millis(V, U) when U == m; U == <<"m">> -> V*1000*60;
unit_to_millis(V, U) when U == h; U == <<"h">> -> V*1000*60*60;
unit_to_millis(V, U) when U == d; U == <<"d">> -> V*1000*60*60*24;
unit_to_millis(_, _) -> error.

%% @doc Return the time in milliseconds since 00:00 GMT Jan 1, 1970 (Unix Epoch)
-spec timestamp_to_ms(erlang:timestamp()) -> time_ms().
timestamp_to_ms({Mega, Secs, Micro}) ->
    Mega*1000000000 + Secs*1000 + Micro div 1000.

%% @doc Return an erlang:timestamp() given the time in milliseconds since the Unix Epoch
-spec ms_to_timestamp(time_ms()) -> erlang:timestamp().
ms_to_timestamp(Time) ->
    Seconds = Time div 1000,
    MicroSeconds = (Time rem 1000) * 1000,
    {0, Seconds, MicroSeconds}.

-spec unit_to_ms(time_unit()) -> time_ms().
unit_to_ms(ms) ->
    1;
unit_to_ms(s) ->
    1000;
unit_to_ms(m) ->
    60 * unit_to_ms(s);
unit_to_ms(h) ->
    60 * unit_to_ms(m);
unit_to_ms(d) ->
    24 * unit_to_ms(h).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

assert_minutes(Quanta, OkTimes) ->
    Time = timestamp_to_ms(os:timestamp()),
    QuantaMs = quantum(Time, Quanta, m),
    {_, {_, M, _}} = calendar:now_to_universal_time(ms_to_timestamp(QuantaMs)),
    ?assert(lists:member(M, OkTimes)).

quantum_minutes_test() ->
    assert_minutes(15, [0, 15, 30, 45]),
    assert_minutes(75, [0, 15, 30, 45]),
    assert_minutes(5, [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]),
    assert_minutes(6, [0, 6, 12, 18, 24, 30, 36, 42, 48, 54]).

assert_hours(Quanta, OkTimes) ->
    Time = timestamp_to_ms(os:timestamp()),
    QuantaMs = quantum(Time, Quanta, h),
    {_, {H, _, _}} = calendar:now_to_universal_time(ms_to_timestamp(QuantaMs)),
    ?assert(lists:member(H, OkTimes)).

quantum_hours_test() ->
    assert_hours(12, [0, 12]),
    assert_hours(24, [0]).

assert_days(Days) ->
    Now = os:timestamp(),
    Time = timestamp_to_ms(Now),
    QuantaMs = quantum(Time, Days, d),
    {NowDate, _} = calendar:now_to_universal_time(Now),
    {QuantaDate, _} = calendar:now_to_universal_time(ms_to_timestamp(QuantaMs)),
    NowDays = calendar:date_to_gregorian_days(NowDate),
    QuantaDays = calendar:date_to_gregorian_days(QuantaDate),
    ?assert((NowDays - QuantaDays) < Days),
    ?assert((NowDays - QuantaDays) >= 0).

quantum_days_test() ->
    assert_days(1),
    assert_days(10),
    assert_days(15),
    assert_days(28),
    assert_days(29),
    assert_days(30),
    assert_days(31).

%%
%% test Quanta
%%

-define(MIN, 60*1000). % minute in miliseconds
single_quanta_test() ->
    Start = 1 * ?MIN,
    End   = 5 * ?MIN,
    {N, Quanta} = quanta(Start, End, 15, m),
    Length = length(Quanta),
    ?assertEqual(1, N),
    ?assertEqual(N - 1, Length),
    ?assertEqual([], Quanta).

two_quanta_test() ->
    Start = 1 * ?MIN,
    End   = 16 * ?MIN,
    {N, Quanta} = quanta(Start, End, 15, m),
    Length = length(Quanta),
    ?assertEqual(2, N),
    ?assertEqual(N -1, Length),
    ?assertEqual([15 * ?MIN], Quanta).

split_quanta_test() ->
    Start = 14 * ?MIN,
    End   = 16 * ?MIN,
    {N, Quanta} = quanta(Start, End, 15, m),
    Length = length(Quanta),
    ?assertEqual(2, N),
    ?assertEqual(N - 1, Length),
    ?assertEqual([15 * ?MIN], Quanta).

-ifdef(EQC).
prop_quantum_bounded_test() ->
    ?assertEqual(
       true,
       eqc:quickcheck(
         eqc:numtests(1000, prop_quantum_bounded()))
      ).

%% Ensure that Quantas are always bounded, meaning that any time is no more
%% than one quantum ahead of the quantum start.
prop_quantum_bounded() ->
    ?FORALL(
       {Date, Time, {Quanta, Unit}},
       {date_gen(), time_gen(), quantum_gen()},
       begin
           DateTime = {Date, Time},
           SecondsFrom0To1970 = ?DAYS_FROM_0_TO_1970 * (unit_to_ms(d) div 1000),
           DateMs = (calendar:datetime_to_gregorian_seconds(DateTime) - SecondsFrom0To1970)*1000,
           QuantaMs = quantum(DateMs, Quanta, Unit),
           QuantaSize = quantum_in_ms(Quanta, Unit),
           (DateMs - QuantaMs) =< QuantaSize
       end).

quantum_now_from_datetime(DateTime, Quanta, Unit) ->
    SecondsFrom0To1970 = ?DAYS_FROM_0_TO_1970 * (unit_to_ms(d) div 1000),
    DateMs = (calendar:datetime_to_gregorian_seconds(DateTime) - SecondsFrom0To1970)*1000,
    QuantaMs = quantum(DateMs, Quanta, Unit),
    ms_to_timestamp(QuantaMs).

quantum_in_ms(Quanta, Unit) ->
    Quanta*unit_to_ms(Unit).

%% EQC Generators
date_gen() ->
    ?SUCHTHAT(Date, {choose(1970, 2015), choose(1, 12), choose(1, 31)}, calendar:valid_date(Date)).

time_gen() ->
    {choose(0, 23), choose(0, 59), choose(0, 59)}.

%% We expect quanta to be bigger than their cardinality
%% A quantum of 100 minutes is perfectly reasonable
quantum_gen() ->
    oneof([ 
            {choose(1, 1000), d},
            {choose(1, 1000), h},
            {choose(1, 1000), m},
            {choose(1, 1000), s}
          ]).

-endif.
-endif.
