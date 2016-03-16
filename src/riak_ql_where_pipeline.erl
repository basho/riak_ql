%% -------------------------------------------------------------------
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

%% @doc This document implements the riak_ql SQL Where clause 
%% in the query pipeline
-module(riak_ql_where_pipeline).

-export([
         make_where/2,
         get_version/0
]).

%% for testing
-export([
         canonicalise_where_TEST/1
        ]).

%%
%% API
%%
make_where({where, A}, {expr, B}) ->
    NewB = riak_ql_parser_util:remove_exprs(B),
    {A, [canonicalise_where(NewB)]}.

get_version() -> "1.3".

%%
%% Internal Functions
%%

%%
%% rewrite the where clause to have a canonical form
%% makes query rewriting easier
%%
canonicalise_where(WhereClause) ->
    Canonical = canon2(WhereClause),
    _NewWhere = hoist(Canonical).

canon2({Cond, A, B}) when is_binary(B) andalso not is_binary(A) ->
    canonicalize_condition_order({Cond, B, A});
canon2({Cond, A, B}) when Cond =:= and_ orelse
                          Cond =:= or_  ->
    %% this is stack busting non-tail recursion
    %% but our where clauses are bounded in size so thats OK
    A1 = canon2(A),
    B1 = canon2(B),
    case A1 == B1 of
        true ->
            A1;
        false ->
            case is_lower(A1, B1) of
                true  -> {Cond, A1, B1};
                false -> {Cond, B1, A1}
            end
    end;
canon2({Op, A, B}) ->
    {Op, strip(A), strip(B)};
canon2(A) ->
    A.

-spec canonicalize_condition_order({atom(), any(), binary()}) -> {atom(), binary(), any()}.
canonicalize_condition_order({'>', Reference, Column}) ->
    canon2({'<', Column, Reference});
canonicalize_condition_order({'<', Reference, Column}) ->
    canon2({'>', Column, Reference}).

strip({identifier, A}) -> A;
strip(A)               -> A.

hoist({and_, {and_, A, B}, C}) ->
    Hoisted = {and_, A, hoist({and_, B, C})},
    _Sort = sort(Hoisted);
hoist({A, B, C}) ->
    B2 = case B of
             {and_, _, _} -> hoist(B);
             _            -> B
         end,
    C2 = case C of
             {and_, _, _} -> hoist(C);
             _            -> C
         end,
    {A, B2, C2}.

%% a truly horendous bubble sort algo which is also
%% not tail recursive
sort({and_, A, {and_, B, C}}) ->
    case is_lower(A, B) of
        true ->
            {and_, B1, C1} = sort({and_, B, C}),
            case is_lower(A, B1) of
                true  -> {and_, A, {and_, B1, C1}};
                false -> sort({and_, B1, {and_, A, C1}})
            end;
        false ->
            sort({and_, B, sort({and_, A, C})})
    end;
sort({and_, A, A}) ->
    A;
sort({Op, A, B}) ->
    case is_lower(A, B) of
        true  -> {Op, A, B};
        false -> {Op, B, A}
    end.

is_lower(Ands, {_, _, _}) when is_list(Ands)->
    true;
is_lower({_, _, _}, Ands) when is_list(Ands)->
    true;
is_lower(Ands1, Ands2) when is_list(Ands1) andalso is_list(Ands2) ->
    true;
is_lower({Op1, _, _} = A, {Op2, _, _} = B) when (Op1 =:= and_ orelse
                                         Op1 =:= or_  orelse
                                         Op1 =:= '>'  orelse
                                         Op1 =:= '<'  orelse
                                         Op1 =:= '>=' orelse
                                         Op1 =:= '<=' orelse
                                         Op1 =:= '='  orelse
                                         Op1 =:= '<>' orelse
                                         Op1 =:= '=~' orelse
                                         Op1 =:= '!~' orelse
                                         Op1 =:= '!=')
                                        andalso
                                        (Op2 =:= and_ orelse
                                         Op2 =:= or_  orelse
                                         Op2 =:= '>'  orelse
                                         Op2 =:= '<'  orelse
                                         Op2 =:= '>=' orelse
                                         Op2 =:= '<=' orelse
                                         Op2 =:= '='  orelse
                                         Op2 =:= '<>' orelse
                                         Op2 =:= '=~' orelse
                                         Op2 =:= '!~' orelse
                                         Op2 =:= '!=') ->
    (A =< B).

%%
%% Test interfaces
%%
canonicalise_where_TEST(WhereClause) -> canonicalise_where(WhereClause).
