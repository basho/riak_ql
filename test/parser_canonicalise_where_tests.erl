%% -------------------------------------------------------------------
%%
%% Canonical WHERE clause tests for the Parser
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

-module(parser_canonicalise_where_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

%%
%% canonicalise WHERE clauses tests
%%

canonicalise_where_1_test() ->
    ?where_test({or_,
                 {'<', "alpha", {integer, 2}},
                 {'>', "beta",  {integer, 3}}
                },
                {or_,
                 {'<', "alpha", {integer, 2}},
                 {'>', "beta",  {integer, 3}}
                }).

canonicalise_where_2_test() ->
    ?where_test({or_,
                 {'>', "beta",  {integer, 3}},
                 {'<', "alpha", {integer, 2}}
                },
                {or_,
                 {'<', "alpha", {integer, 2}},
                 {'>', "beta",  {integer, 3}}
                }).

canonicalise_where_2a_test() ->
    ?where_test({or_,
                 {'>=', "beta",  {integer, 3}},
                 {'<', "alpha", {integer, 2}}
                },
                {or_,
                 {'<', "alpha", {integer, 2}},
                 {'>=', "beta",  {integer, 3}}
                }).

canonicalise_where_2b_test() ->
    ?where_test({or_,
                 {'>', "beta",  {integer, 3}},
                 {'<=', "alpha", {integer, 2}}
                },
                {or_,
                 {'<=', "alpha", {integer, 2}},
                 {'>', "beta",  {integer, 3}}
                }).


canonicalise_where_3_test() ->
    ?where_test({and_,
                 {'>', "beta",  {integer, 3}},
                 {'<', "alpha", {integer, 2}}
                },
                {and_,
                 {'<', "alpha", {integer, 2}},
                 {'>', "beta",  {integer, 3}}
                }).

canonicalise_where_4_test() ->
    ?where_test({or_,
                 {and_,
                  {'>', "beta",  {integer, 3}},
                  {'<', "alpha", {integer, 2}}
                 },
                 {'=', "time", {integer, 1234}}
                },
                {or_,
                 {'=', "time", {integer, 1234}},
                 {and_,
                  {'<', "alpha", {integer, 2}},
                  {'>', "beta",  {integer, 3}}
                 }
                }).

%%
%% these are the ones that matters
%% all the ands float to the front which means
%% the query rewriter can walk them them and rearange them
%%
canonicalise_where_5_test() ->
    ?where_test({and_,
                 {or_,
                  {'>', "beta",  {integer, 3}},
                  {'<', "alpha", {integer, 2}}
                 },
                 {and_,
                  {'>', "gamma", {integer, 3}},
                  {'<', "delta", {integer, 2}}
                 }
                },
                {and_,
                 {'<', "delta", {integer, 2}},
                 {and_,
                  {'>', "gamma", {integer, 3}},
                  {or_,
                   {'<', "alpha", {integer, 2}},
                   {'>', "beta",  {integer, 3}}
                  }
                 }
                }).

canonicalise_where_6_test() ->
    ?where_test({and_,
                 {and_,
                  {'>', "beta6",  {integer, 3}},
                  {'<', "alpha6", {integer, 2}}
                 },
                 {and_,
                  {'>', "gamma6", {integer, 3}},
                  {'<', "delta6", {integer, 2}}
                 }
                },
                {and_,
                 {'<', "alpha6", {integer, 2}},
                 {and_,
                  {'<', "delta6", {integer, 2}},
                  {and_,
                   {'>', "beta6",  {integer, 3}},
                   {'>', "gamma6", {integer, 3}}
                  }
                 }
                }).

canonicalise_where_7_test() ->
    ?where_test({and_,
                 {and_,
                  {or_,
                   {'>', "beta7",  {integer, 3}},
                   {'<', "alpha7", {integer, 2}}
                  },
                  {and_,
                   {'>', "gamma7", {integer, 3}},
                   {'<', "delta7", {integer, 2}}
                  }
                 },
                 {and_,
                  {'>', "epsilon7", {integer, 3}},
                  {'<', "zeta7",    {integer, 2}}
                 }
                },
                {and_,
                 {'<', "delta7", {integer, 2}},
                 {and_,
                  {'<', "zeta7", {integer, 2}},
                  {and_,
                   {'>', "epsilon7", {integer, 3}},
                   {and_,
                    {'>', "gamma7", {integer, 3}},
                    {or_,
                     {'<', "alpha7", {integer, 2}},
                     {'>', "beta7",  {integer, 3}}
                    }
                   }
                  }
                 }
                }).
