-module(parser_basic_1).

-include_lib("eunit/include/eunit.hrl").

-include("riak_ql_ddl.hrl").

-compile(export_all).

-define(assert_partition_key_in_query (Partion_key, Query),
	Tokens = riak_ql_lexer:get_tokens(Query),
	{ok, Got} = riak_ql_parser:parse(Tokens),
	?assertEqual(Partion_key, Got#ddl_v1.partition_key)
).

% partition_key_1_test() ->
% 	?assert_partition_key_in_query(
% 		#key_v1{ ast = [#param_v1{ name = [<<"v">>] }] },
% 		"create table mytab (v varchar not null, temperature_k timestamp, primary key (v))"
% 	).

% partition_key_2_test() ->
% 	?assert_partition_key_in_query(
% 		#key_v1{ ast = [#param_v1{ name = [<<"f">>] }] },
% 		"create table mytab (f float not null, temperature_k timestamp, primary key (f))"
% 	).

% partition_key_3_test() ->
% 	?assert_partition_key_in_query(
% 		#key_v1{ ast = [#param_v1{ name = [<<"t">>] }] },
% 		"create table mytab (t timestamp not null, temperature_k timestamp, primary key (t))"
% 	).

% partition_key_4_test() ->
% 	?assert_partition_key_in_query(
% 		#key_v1{ ast = [#param_v1{ name = [<<"v">>] }, #param_v1{ name = [<<"t">>] }] },
% 		"create table mytab (v timestamp not null, t timestamp, primary key (v, t))"
% 	).

% partition_key_5_test() ->
% 	?assert_partition_key_in_query(
% 		#key_v1{ ast = [#param_v1{ name = [<<"v">>] }] },
% 		"create table mytab (v varchar not null, primary key (v))"
% 	).

% partition_key_6_test() ->
% 	?assert_partition_key_in_query(
% 		#key_v1{ ast = [#param_v1{ name = [<<"t">>] }] },
% 		"create table mytab (t timestamp not null, primary key (t))"
% 	).

% -define(assert_local_key_in_query (Local_key, Query),
% 	Tokens = riak_ql_lexer:get_tokens(Query),
% 	{ok, Got} = riak_ql_parser:parse(Tokens),
% 	?assertEqual(Local_key, Got#ddl_v1.local_key)
% ).

% local_key_1_test() ->
% 	?assert_local_key_in_query(
% 		#key_v1{ ast = [#param_v1{ name = [<<"v">>] }] },
% 		"create table temperatures (v varchar not null, temperature_k timestamp, primary key ((v), v))"
% 	).

% local_key_2_test() ->
% 	?assert_local_key_in_query(
% 		#key_v1{ ast = [#param_v1{ name = [<<"v">>] }] },
% 		"create table temperatures (v varchar not null, primary key (v))"
% 	).
