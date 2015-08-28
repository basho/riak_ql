%% -------------------------------------------------------------------
%%
%% riak_ql_ddl: API module for the DDL
%%
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
-module(riak_ql_ddl).

-include("riak_ql_sql.hrl").
-include("riak_ql_ddl.hrl").

%% this function can be used to work out which Module to use
-export([
	 make_module_name/1
	]).

%% a helper function for destructuring data objects
%% and testing the validity of field names
%% the generated helper functions cannot contain
%% record definitions because of the build cycle
%% so this function can be called out to to pick
%% apart the DDL records
-export([
	 get_partition_key/2,
	 get_local_key/2,
	 make_key/3,
	 is_valid_field/2,
	 is_query_valid/2
	]).

-ifdef(TEST).
%% for debugging only
-export([
	 make_ddl/2,
	 are_selections_valid/3
	]).
-endif.

-define(CANBEBLANK,  true).
-define(CANTBEBLANK, false).

-type bucket()                :: binary().
-type modulename()            :: atom().
-type heirarchicalfieldname() :: [string()].

-spec make_module_name(bucket()) -> modulename().
make_module_name(Bucket) when is_binary(Bucket) ->
    Nonce = binary_to_list(base64:encode(crypto:hash(md4, Bucket))),
    Nonce2 = remove_hooky_chars(Nonce),
    ModName = "riak_ql_ddl_helper_mod_" ++ Nonce2,
    list_to_atom(ModName).

-spec get_partition_key(#ddl_v1{}, tuple()) -> term().
get_partition_key(#ddl_v1{bucket = B, partition_key = PK}, Obj)
  when is_tuple(Obj) ->
    Mod = make_module_name(B),
    #key_v1{ast = Params} = PK,
    _Key = build(Params, Obj, Mod, []).

-spec get_local_key(#ddl_v1{}, tuple()) -> term().
get_local_key(#ddl_v1{bucket = B, local_key = LK}, Obj) 
  when is_tuple(Obj) ->
    Mod = make_module_name(B),
    #key_v1{ast = Params} = LK,
    _Key = build(Params, Obj, Mod, []).

-spec make_key(atom(), #key_v1{} | none, list()) -> [{atom(), any()}]. 
make_key(_Mod, none, _Vals) -> [];
make_key(Mod, #key_v1{ast = AST}, Vals) when is_atom(Mod)  andalso 
					     is_list(Vals) ->
    mk_k(AST, Vals, Mod, []).

%% TODO there is a mismatch between how the fields in the where clause
%% and the fields in the DDL are mapped
mk_k([], _Vals, _Mod, Acc) ->
    lists:reverse(Acc);
mk_k([#hash_fn_v1{mod = Md,
		 fn   = Fn,
		 args = Args,
		 type = Ty} | T1], Vals, Mod, Acc) ->
    A2 = extract(Args, Vals, []),
    V  = erlang:apply(Md, Fn, A2),
    mk_k(T1, Vals, Mod, [{Ty, V} | Acc]);
mk_k([#param_v1{name = [Nm]} | T1], Vals, Mod, Acc) ->
    {Nm, V} = lists:keyfind(Nm, 1, Vals),
    Ty = Mod:get_field_type([Nm]),
    mk_k(T1, Vals, Mod, [{Ty, V} | Acc]).

-spec extract(list(), [{any(), any()}], [any()]) -> any().
extract([], _Vals, Acc) ->
    lists:reverse(Acc);
extract([#param_v1{name = [Nm]} | T], Vals, Acc) ->
    {Nm, Val} = lists:keyfind(Nm, 1, Vals),
    extract(T, Vals, [Val | Acc]);
extract([Constant | T], Vals, Acc) ->
    extract(T, Vals, [Constant | Acc]).

-spec build([#param_v1{}], tuple(), atom(), any()) -> list().
build([], _Obj, _Mod, A) ->
    lists:reverse(A);
build([#param_v1{name = Nm} | T], Obj, Mod, A) ->
    Val = Mod:extract(Obj, Nm),
    Type = Mod:get_field_type(Nm),
    build(T, Obj, Mod, [{Type, Val} | A]);
build([#hash_fn_v1{mod  = Md,
		   fn   = Fn,
		   args = Args,
		   type = Ty} | T], Obj, Mod, A) ->
    A2 = convert(Args, Obj, Mod, []),
    Val = erlang:apply(Md, Fn, A2),
    build(T, Obj, Mod, [{Ty, Val} | A]).

-spec convert([#param_v1{}], tuple(), atom(), [any()]) -> any().
convert([], _Obj, _Mod, Acc) ->
    lists:reverse(Acc);
convert([#param_v1{name = Nm} | T], Obj, Mod, Acc) ->
    Val = Mod:extract(Obj, Nm),
    convert(T, Obj, Mod, [Val | Acc]);
convert([Constant | T], Obj, Mod, Acc) ->
    convert(T, Obj, Mod, [Constant | Acc]).

-spec is_valid_field(#ddl_v1{}, heirarchicalfieldname()) -> boolean().
is_valid_field(#ddl_v1{bucket = B}, Field) when is_list(Field)->
    Mod = riak_ql_ddl:make_module_name(B),
    Mod:is_field_valid(Field).

is_query_valid(#ddl_v1{bucket = B} = DDL,
	       #riak_sql_v1{'FROM'   = B,
			    'SELECT' = S,
			    'WHERE'  = F}) ->
    ValidSelection = are_selections_valid(DDL, S, ?CANTBEBLANK),
    ValidFilters   = are_filters_valid(DDL, F),
    case {ValidSelection, ValidFilters} of
	{true, true} -> true;
	_            -> {false, [
				 {are_selections_valid, ValidSelection},
				 {are_filters_valid,    ValidFilters}
				]}
    end;
is_query_valid(#ddl_v1{bucket = B1}, #riak_sql_v1{'FROM' = B2}) ->
    Msg = io_lib:format("DDL has a bucket of ~p "
			"but query has a bucket of ~p~n", [B1, B2]),
    {false, {ddl_mismatch, lists:flatten(Msg)}}.

are_filters_valid(#ddl_v1{}, []) ->
    true;
are_filters_valid(#ddl_v1{} = DDL, Filters) ->
    Fields = extract_fields(Filters),
    are_selections_valid(DDL, Fields, ?CANBEBLANK).

are_selections_valid(#ddl_v1{}, [], ?CANTBEBLANK) ->
    {false, [{selections_cant_be_blank, []}]};
are_selections_valid(#ddl_v1{} = DDL, Selections, _) ->
    CheckFn = fun(X, {Acc, Status}) ->
		      case is_valid_field(DDL, X) of
			  true  -> {Acc, Status};
			  false -> Msg = {invalid_field, X},
				   {[Msg | Acc], false}
		      end
	      end,
    case lists:foldl(CheckFn, {[], true}, Selections) of
	{[],   true}  -> true;
	{Msgs, false} -> {false, Msgs}
    end.

extract_fields(Fields) ->
    extract_f2(Fields, []).

extract_f2([], Acc) ->
    lists:reverse(Acc);
extract_f2([{Op, LHS, RHS} | T], Acc) when Op =:= '='    orelse
					   Op =:= 'and_' orelse
					   Op =:= 'or_'  orelse
					   Op =:= '>'    orelse
					   Op =:= '<'    orelse
					   Op =:= '=<'   orelse
					   Op =:= '>=' ->
    %% you have to double wrap the variable name inside the list of names
    Acc2 = case is_tuple(LHS) of
	       true  -> extract_f2([LHS], Acc);
	       false -> [[LHS] | Acc]
	   end,
    %% note that the RHS is treated differently to the LHS - the LHS is always
    %% a field - but the RHS terminates on a value
    Acc3 = case is_val(RHS) of
	       false -> extract_f2([RHS], Acc2);
	       true  -> Acc2
	   end,
    extract_f2(T, Acc3).

is_val({word,     _}) -> true;
is_val({int,      _}) -> true;
is_val({float,    _}) -> true; 
is_val({datetime, _}) -> true; 
is_val({varchar,  _}) -> true; 
is_val(_)             -> false.

remove_hooky_chars(Nonce) ->
    re:replace(Nonce, "[/|\+|\.|=]", "", [global, {return, list}]).

-ifdef(TEST).
-compile(export_all).

-define(VALID,   true).
-define(INVALID, false).

-include_lib("eunit/include/eunit.hrl").

%%
%% Helper Fn for unit tests
%%

mock_partition_fn(_A, _B, _C) -> mock_result.

make_ddl(Bucket, Fields) when is_binary(Bucket) ->
    make_ddl(Bucket, Fields, #key_v1{}, #key_v1{}).

make_ddl(Bucket, Fields, PK) when is_binary(Bucket) ->
    make_ddl(Bucket, Fields, PK, #key_v1{}).

make_ddl(Bucket, Fields, #key_v1{} = PK, #key_v1{} = LK)
  when is_binary(Bucket) ->
    #ddl_v1{bucket        = Bucket,
	    fields        = Fields,
	    partition_key = PK,
	    local_key     = LK}.

%%
%% get partition_key tests
%%

simplest_partition_key_test() ->
    Name = "yando",
    PK = #key_v1{ast = [
			#param_v1{name = [Name]}
		       ]},
    DDL = make_ddl(<<"simplest_partition_key_test">>,
		   [
		    #riak_field_v1{name     = Name,
				   position = 1,
				   type     = binary}
		   ],
		   PK),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Obj = {<<"yarble">>},
    Result = (catch get_partition_key(DDL, Obj)),
    ?assertEqual([{binary, <<"yarble">>}], Result).

simple_partition_key_test() ->
    Name1 = "yando",
    Name2 = "buckle",
    PK = #key_v1{ast = [
			#param_v1{name = [Name1]},
			#param_v1{name = [Name2]}
		       ]},
    DDL = make_ddl(<<"simple_partition_key_test">>,
		   [
		    #riak_field_v1{name     = Name2,
				   position = 1,
				   type     = binary},
		    #riak_field_v1{name     = "sherk",
				   position = 2,
				   type     = binary},
		    #riak_field_v1{name     = Name1,
				   position = 3,
				   type     = binary}
		   ],
		   PK),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Obj = {<<"one">>, <<"two">>, <<"three">>},
    Result = (catch get_partition_key(DDL, Obj)),
    ?assertEqual([{binary, <<"three">>}, {binary, <<"one">>}], Result).

function_partition_key_test() ->
    Name1 = "yando",
    Name2 = "buckle",
    PK = #key_v1{ast = [
			#param_v1{name = [Name1]},
			#hash_fn_v1{mod  = ?MODULE,
				    fn   = mock_partition_fn,
				    args = [
					    #param_v1{name = [Name2]},
					    15,
					    m
					   ],
				    type = timestamp
				   }
		       ]},
    DDL = make_ddl(<<"function_partition_key_test">>,
		   [
		    #riak_field_v1{name     = Name2,
				   position = 1,
				   type     = timestamp},
		    #riak_field_v1{name     = "sherk",
				   position = 2,
				   type     = binary},
		    #riak_field_v1{name     = Name1,
				   position = 3,
				   type     = binary}
		   ],
		   PK),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Obj = {1234567890, <<"two">>, <<"three">>},
    Result = (catch get_partition_key(DDL, Obj)),
    %% Yes the mock partition function is actually computed
    %% read the actual code, lol
    Expected = [{binary, <<"three">>}, {timestamp, mock_result}],
    ?assertEqual(Expected, Result).

complex_partition_key_test() ->
    Name0 = "yerp",
    Name1 = "yando",
    Name2 = "buckle",
    Name3 = "doodle",
    PK = #key_v1{ast = [
			#param_v1{name = [Name0, Name1]},
			#hash_fn_v1{mod  = ?MODULE,
				    fn   = mock_partition_fn,
				    args = [
					    #param_v1{name = [
							      Name0,
							      Name2,
							      Name3
							     ]},
					    "something",
					    pong
					   ],
				    type = poodle
				   },
			#hash_fn_v1{mod  = ?MODULE,
				    fn   = mock_partition_fn,
				    args = [
					    #param_v1{name = [
							      Name0,
							      Name1
							     ]},
					    #param_v1{name = [
							      Name0,
							      Name2,
							      Name3
							     ]},
					    pang
					   ],
				    type = wombat
				   }
		       ]},
    Map3 = {map, [
		  #riak_field_v1{name     = "in_Map_2",
				 position = 1,
				 type     = integer}
		 ]},
    Map2 = {map, [
		  #riak_field_v1{name     = Name3,
				 position = 1,
				 type     = integer}
		 ]},
    Map1 = {map, [
		  #riak_field_v1{name     = Name1,
				 position = 1,
				 type     = integer},
		  #riak_field_v1{name     = Name2,
				 position = 2,
				 type     = Map2},
		  #riak_field_v1{name     = "Level_1_map2",
				 position = 3,
				 type     = Map3}
		 ]},
    DDL = make_ddl(<<"complex_partition_key_test">>,
		   [
		    #riak_field_v1{name     = Name0,
				   position = 1,
				   type     = Map1}
		   ],
		   PK),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Obj = {{2, {3}, {4}}},
    Result = (catch get_partition_key(DDL, Obj)),
    Expected = [{integer, 2}, {poodle, mock_result}, {wombat, mock_result}],
    ?assertEqual(Expected, Result).

%%
%% get local_key tests
%%

simplest_local_key_test() ->
    Name = "yando",
    PK = #key_v1{ast = [
			#param_v1{name = [Name]}
		       ]},
    LK = #key_v1{ast = [
			#param_v1{name = [Name]}
		       ]},
    DDL = make_ddl(<<"simplest_key_key_test">>,
		   [
		    #riak_field_v1{name     = Name,
				   position = 1,
				   type     = binary}
		   ],
		   PK, LK),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Obj = {<<"yarble">>},
    Result = (catch get_local_key(DDL, Obj)),
    ?assertEqual([{binary, <<"yarble">>}], Result).

%%
%% get type of named field
%%

simplest_valid_get_type_test() ->
    DDL = make_ddl(<<"simplest_valid_get_type_test">>,
		   [
		    #riak_field_v1{name     = "yando",
				   position = 1,
				   type     = binary}
		   ]),
    {module, Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Result = (catch Module:get_field_type(["yando"])),
    ?assertEqual(binary, Result).

simple_valid_get_type_test() ->
    DDL = make_ddl(<<"simple_valid_get_type_test">>,
		   [
		    #riak_field_v1{name     = "scoobie",
				   position = 1,
				   type     = integer},
		    #riak_field_v1{name     = "yando",
				   position = 2,
				   type     = binary}
		   ]),
    {module, Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Result = (catch Module:get_field_type(["yando"])),
    ?assertEqual(binary, Result).

simple_valid_map_get_type_1_test() ->
    Map = {map, [
		 #riak_field_v1{name     = "yarple",
				position = 1,
				type     = integer}
		]},
    DDL = make_ddl(<<"simple_valid_map_get_type_1_test">>,
		   [
		    #riak_field_v1{name     = "yando",
				   position = 1,
				   type     = binary},
		    #riak_field_v1{name     = "erko",
				   position = 2,
				   type     = Map},
		    #riak_field_v1{name     = "erkle",
				   position = 3,
				   type     = float}
		   ]),
    {module, Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Result = (catch Module:get_field_type(["erko", "yarple"])),
    ?assertEqual(integer, Result).

simple_valid_map_get_type_2_test() ->
    Map = {map, [
		 #riak_field_v1{name     = "yarple",
				position = 1,
				type     = integer}
		]},
    DDL = make_ddl(<<"simple_valid_map_get_type_2_test">>,
		   [
		    #riak_field_v1{name     = "yando",
				   position = 1,
				   type     = binary},
		    #riak_field_v1{name     = "erko",
				   position = 2,
				   type     = Map},
		    #riak_field_v1{name     = "erkle",
				   position = 3,
				   type     = float}
		   ]),
    {module, Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Result = (catch Module:get_field_type(["erko"])),
    ?assertEqual(map, Result).

complex_valid_map_get_type_test() ->
    Map3 = {map, [
		  #riak_field_v1{name     = "in_Map_2",
				 position = 1,
				 type     = integer}
		 ]},
    Map2 = {map, [
		  #riak_field_v1{name     = "in_Map_1",
				 position = 1,
				 type     = integer}
		 ]},
    Map1 = {map, [
		  #riak_field_v1{name     = "Level_1_1",
				 position = 1,
				 type     = integer},
		  #riak_field_v1{name     = "Level_1_map1",
				 position = 2,
				 type     = Map2},
		  #riak_field_v1{name     = "Level_1_map2",
				 position = 3,
				 type     = Map3}
		 ]},
    DDL = make_ddl(<<"complex_valid_map_get_type_test">>,
		   [
		    #riak_field_v1{name     = "Top_Map",
				   position = 1,
				   type     = Map1}
		   ]),
    {module, Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Path = ["Top_Map", "Level_1_map1", "in_Map_1"],
    Res = (catch Module:get_field_type(Path)),
    ?assertEqual(integer, Res).

%%
%% make_key tests
%%

make_plain_key_test() ->
    Key = #key_v1{ast = [
			 #param_v1{name = ["user"]},
			 #param_v1{name = ["time"]}
			]},
    DDL = make_ddl(<<"make_plain_key_test">>,
		   [
		    #riak_field_v1{name     = "user",
				   position = 1,
				   type     = binary},
		    #riak_field_v1{name     = "time",
				   position = 2,
				   type     = timestamp}
		   ],
		   Key, %% use the same key for both
		   Key),
    Time = 12345,
    Vals = [
	    {"user", "user_1"},
	    {"time", Time}
	   ],
    {module, Mod} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Got = make_key(Mod, Key, Vals),
    Expected = [{binary, "user_1"}, {timestamp, Time}],
    ?assertEqual(Expected, Got).    

make_functional_key_test() ->
    Key = #key_v1{ast = [
			 #param_v1{name = ["user"]},
			 #hash_fn_v1{mod  = ?MODULE,
				     fn   = mock_partition_fn,
				     args = [
					     #param_v1{name = ["time"]},
					     15,
					     m
					    ],
				     type = timestamp
				    }
			]},
    DDL = make_ddl(<<"make_plain_key_test">>,
		   [
		    #riak_field_v1{name     = "user",
				   position = 1,
				   type     = binary},
		    #riak_field_v1{name     = "time",
				   position = 2,
				   type     = timestamp}
		   ],
		   Key, %% use the same key for both
		   Key),
    Time = 12345,
    Vals = [
	    {"user", "user_1"},
	    {"time", Time}
	   ],
    {module, Mod} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Got = make_key(Mod, Key, Vals),
    Expected = [{binary, "user_1"}, {timestamp, mock_result}],
    ?assertEqual(Expected, Got).    


%%
%% Validate Query Tests
%%

partial_are_selections_valid_test() ->
    Selections  = [["temperature"], ["geohash"]],
    DDL = make_ddl(<<"partial_are_selections_valid_test">>,
		   [
		    #riak_field_v1{name     = "temperature",
				   position = 1,
				   type     = integer},
		    #riak_field_v1{name     = "geohash",
				   position = 2,
				   type     = integer}
		   ]),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Res = are_selections_valid(DDL, Selections, ?CANTBEBLANK),
    ?assertEqual(true, Res).

partial_wildcard_are_selections_valid_test() ->
    Selections  = [["*"]],
    DDL = make_ddl(<<"partial_wildcard_are_selections_valid_test">>,
		   [
		    #riak_field_v1{name     = "temperature",
				   position = 1,
				   type     = integer},
		    #riak_field_v1{name     = "geohash",
				   position = 2,
				   type     = integer}
		   ]),
    {module, _Module} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Res = are_selections_valid(DDL, Selections, ?CANTBEBLANK),
    ?assertEqual(true, Res).

partial_are_selections_valid_fail_test() ->
    Selections  = [],
    DDL = make_ddl(<<"partial_are_selections_valid_fail_test">>,
		   [
		    #riak_field_v1{name     = "temperature",
				   position = 1,
				   type     = integer},
		    #riak_field_v1{name     = "geohash",
				   position = 2,
				   type     = integer}
		   ]),
    {Res, _} = are_selections_valid(DDL, Selections, ?CANTBEBLANK),
    ?assertEqual(false, Res).

%%
%% Query Validation tests
%%

simple_is_query_valid_test() ->
    Bucket = <<"simple_is_query_valid_test">>,
    Selections  = [["temperature"], ["geohash"]],
    Query = #riak_sql_v1{'FROM'   = Bucket,
			 'SELECT' = Selections},
    DDL = make_ddl(Bucket,
		   [
		    #riak_field_v1{name     = "temperature",
				   position = 1,
				   type     = integer},
		    #riak_field_v1{name     = "geohash",
				   position = 2,
				   type     = integer}
		   ]),
    {module, _ModName} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Res = riak_ql_ddl:is_query_valid(DDL, Query),
    ?assertEqual(true, Res).

simple_is_query_valid_fail_test() ->
    Bucket = <<"simple_is_query_valid_fail_test">>,
    Selections  = [["temperature"], ["yerble"]],
    Query = #riak_sql_v1{'FROM'   = Bucket,
			 'SELECT' = Selections},
    DDL = make_ddl(Bucket,
		   [
		    #riak_field_v1{name     = "temperature",
				   position = 1,
				   type     = integer},
		    #riak_field_v1{name     = "geohash",
				   position = 2,
				   type     = integer}
		   ]),
    {module, _ModName} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    {Res, _} = riak_ql_ddl:is_query_valid(DDL, Query),
    ?assertEqual(false, Res).

simple_is_query_valid_map_test() ->
    Bucket = <<"simple_is_query_valid_map_test">>,
    Name0 = "name",
    Name1 = "temp",
    Name2 = "geo",
    Selections  = [["temp", "geo"], ["name"]],
    Query = #riak_sql_v1{'FROM'   = Bucket,
			 'SELECT' = Selections},
    Map = {map, [
		 #riak_field_v1{name     = Name2,
				position = 1,
				type     = integer}
		]},
    DDL = make_ddl(Bucket,
		   [
		    #riak_field_v1{name     = Name0,
				   position = 1,
				   type     = integer},
		    #riak_field_v1{name     = Name1,
				   position = 2,
				   type     = Map}
		   ]),
    {module, _ModName} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Res = riak_ql_ddl:is_query_valid(DDL, Query),
    ?assertEqual(true, Res).

simple_is_query_valid_map_wildcard_test() ->
    Bucket = <<"simple_is_query_valid_map_wildcard_test">>,
    Name0 = "name",
    Name1 = "temp",
    Name2 = "geo",
    Selections  = [["temp", "*"], ["name"]],
    Query = #riak_sql_v1{'FROM'   = Bucket,
			 'SELECT' = Selections},
    Map = {map, [
		 #riak_field_v1{name     = Name2,
				position = 1,
				type     = integer}
		]},
    DDL = make_ddl(Bucket,
		   [
		    #riak_field_v1{name     = Name0,
				   position = 1,
				   type     = integer},
		    #riak_field_v1{name     = Name1,
				   position = 2,
				   type     = Map}
		   ]),
    {module, _ModName} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Res = riak_ql_ddl:is_query_valid(DDL, Query),
    ?assertEqual(true, Res).

%%
%% Tests for queries with non-null filters
%%
simple_filter_query_test() ->
    Bucket = <<"simple_filter_query_test">>,
    Selections = [["temperature"], ["geohash"]],
    Where = [
	     {and_, 
	      {'>', "temperature", {int, 1}}, 
	      {'<', "temperature", {int, 15}}
	     }
	    ],
    Query = #riak_sql_v1{'FROM'   = Bucket,
			 'SELECT' = Selections,
			 'WHERE'  = Where},
    DDL = make_ddl(Bucket,
		   [
		    #riak_field_v1{name     = "temperature",
				   position = 1,
				   type     = integer},
		    #riak_field_v1{name     = "geohash",
				   position = 2,
				   type     = integer}
		   ]),
    {module, _ModName} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Res = riak_ql_ddl:is_query_valid(DDL, Query),
    ?assertEqual(true, Res).

timeseries_filter_test() ->
    Bucket = <<"timeseries_filter_test">>,
    Selections = [["weather"]],
    Where = [
	     {and_,
	      {and_, 
	       {'>', "time", {int, 3000}},
	       {'<', "time", {int, 5000}}
	      },
	      {'=', "user", {word, "user_1"}
	       }
	      }
	    ],
    Query = #riak_sql_v1{'FROM'   = Bucket,
			 'SELECT' = Selections,
			 'WHERE'  = Where},
    DDL = {ddl_v1,<<"timeseries_filter_test">>,
	   [{riak_field_v1,"geohash",1,binary,false},
	    {riak_field_v1,"user",2,binary,false},
	    {riak_field_v1,"time",3,timestamp,false},
	    {riak_field_v1,"weather",4,binary,false},
	    {riak_field_v1,"temperature",5,binary,true}],
	   {key_v1,[{hash_fn_v1,riak_ql_quanta,quantum,
		     [{param_v1,["time"]},15,s]}]},
	   {key_v1,[{param_v1,["time"]},{param_v1,["user"]}]}},
    {module, _ModName} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Res = riak_ql_ddl:is_query_valid(DDL, Query),
    Expected = true,
    ?assertEqual(Expected, Res).
		    

simple_filter_query_fail_test() ->
    Bucket = <<"simple_filter_query_fail_test">>,
    Selections = [["temperature"], ["geohash"]],
    Where = [
	     {and_, 
	      {'>', "gingerbread", {int, 1}}, 
	      {'<', "temperature", {int, 15}}
	     }
	    ],
    Query = #riak_sql_v1{'FROM'   = Bucket,
			 'SELECT' = Selections,
			 'WHERE'  = Where},
    DDL = make_ddl(Bucket,
		   [
		    #riak_field_v1{name     = "temperature",
				   position = 1,
				   type     = integer},
		    #riak_field_v1{name     = "geohash",
				   position = 2,
				   type     = integer}
		   ]),
    {module, _ModName} = riak_ql_ddl_compiler:make_helper_mod(DDL),
    Res = riak_ql_ddl:is_query_valid(DDL, Query),
    Expected = {false, [
			{are_selections_valid, true},
			{are_filters_valid, {false, [{invalid_field, ["gingerbread"]}]}}
		       ]
	       },
    ?assertEqual(Expected, Res).

-endif.
