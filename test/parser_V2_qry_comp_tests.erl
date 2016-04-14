%% -------------------------------------------------------------------
%%
%% General Parser Tests
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

-module(parser_V2_qry_comp_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

time_unit_seconds_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10s AND time < 20s", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, {{vsn, 2}, [
                                                {and_,
                                                 [
                                                  {'<', [
                                                         {identifier, <<"time">>},
                                                         {integer,20 * 1000}
                                                        ]},
                                                  {'>',[
                                                        {identifier, <<"time">>},
                                                        {integer,10 * 1000}
                                                       ]}
                                                 ]}
                                               ]}}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

time_unit_minutes_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10m AND time < 20m", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, {{vsn, 2}, [
                                                {and_,
                                                 [
                                                  {'<', [
                                                         {identifier, <<"time">>},
                                                         {integer,20 * 60 * 1000}
                                                        ]},
                                                  {'>', [
                                                         {identifier, <<"time">>},
                                                         {integer,10 * 60 * 1000}
                                                        ]}
                                                 ]}
                                               ]}}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

time_unit_seconds_and_minutes_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10s AND time < 20m", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, {{vsn, 2}, [
                                                {and_,
                                                 [
                                                  {'<', [
                                                         {identifier, <<"time">>},
                                                         {integer,20 * 60 * 1000}
                                                        ]},
                                                  {'>', [
                                                         {identifier, <<"time">>},
                                                         {integer,10 * 1000}
                                                        ]}
                                                 ]}
                                               ]}}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

time_unit_hours_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10h AND time < 20h", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, {{vsn, 2}, [
                                                {and_, 
                                                 [
                                                  {'<', [
                                                         {identifier, <<"time">>},
                                                         {integer,20 * 60 * 60 * 1000}
                                                        ]},
                                                  {'>', [
                                                         {identifier, <<"time">>},
                                                         {integer,10 * 60 * 60 * 1000}
                                                        ]}
                                                 ]}
                                               ]}}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

time_unit_days_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10d AND time < 20d", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, {{vsn, 2}, [
                                                {and_,
                                                 [
                                                  {'<',
                                                   [
                                                    {identifier, <<"time">>},
                                                    {integer,20 * 60 * 60 * 24 * 1000}
                                                   ]},
                                                  {'>',
                                                   [
                                                    {identifier, <<"time">>},
                                                    {integer,10 * 60 * 60 * 24 * 1000}
                                                   ]}
                                                 ]}
                                               ]}}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

time_unit_whitespace_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE time > 10   d AND time < 20\td", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, {{vsn, 2}, [
                                                {and_,
                                                 [
                                                  {'<',
                                                   [
                                                    {identifier, <<"time">>},
                                                    {integer,20 * 60 * 60 * 24 * 1000}
                                                   ]},
                                                  {'>',
                                                   [
                                                    {identifier, <<"time">>},
                                                    {integer,10 * 60 * 60 * 24 * 1000}
                                                   ]}
                                                 ]}
                                               ]}}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

left_hand_side_literal_equals_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE 10 = age", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, {{vsn, 2}, [
                                                {'=', 
                                                 [
                                                  {identifier, <<"age">>},
                                                  {integer, 10}
                                                 ]}
                                               ]}}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

left_hand_side_literal_not_equals_test() ->
    ?sql_comp_assert_match("SELECT * FROM mytable WHERE 10 != age", select,
                           [{fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, {{vsn, 2}, [
                                                {'!=', 
                                                 [
                                                  {identifier, <<"age">>}
                                                 , {integer, 10}
                                                 ]}
                                               ]}}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).

%%
%% Regression tests
%%

rts_433_regression_test() ->
    ?sql_comp_assert_match("select * from HardDrivesV14 where date >= 123 "
                           "and date <= 567.0 "
                           "and family = 'Hitachi HDS5C4040ALE630' "
                           "and series = 'true'", select,
                           [
                            {fields, [
                                      {identifier, [<<"*">>]}
                                     ]},
                            {where, {{vsn, 2}, [
                                                {and_,
                                                 [
                                                  {'=', 
                                                   [
                                                    {identifier, <<"series">>}, 
                                                    {binary, <<"true">>}
                                                   ]},
                                                  {and_,
                                                   [
                                                    {'=', 
                                                     [
                                                      {identifier, <<"family">>},
                                                      {binary, <<"Hitachi HDS5C4040ALE630">>}
                                                     ]},
                                                    {and_,
                                                     [
                                                      {'<=', 
                                                       [
                                                        {identifier, <<"date">>}, 
                                                        {float, 567.0}
                                                       ]},
                                                      {'>=', 
                                                       [
                                                        {identifier, <<"date">>}, 
                                                        {integer, 123}
                                                       ]}
                                                     ]}

                                                   ]}
                                                 ]}
                                               ]}}
                           ],
                           {query_compiler, 2}, {query_coordinator, 1}).
