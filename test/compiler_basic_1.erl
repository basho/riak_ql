-module(compiler_basic_1).

-include_lib("eunit/include/eunit.hrl").

%% this is a basic test of timeseries that writes a single element to the back end
%% and checks it is correct

-define(VALID,   true).
-define(INVALID, false).

-compile(export_all).

%%
%% this test calls into the PRIVATE interface
%% mk_helper_m2/1
%%
-define(passing_test(Name, Query, Val, ExpectedPK, ExpectedLK), 
	Name() ->
	       Lexed = riak_ql_lexer:get_tokens(Query),
	       {ok, DDL} = riak_ql_parser:parse(Lexed),
	       case riak_ql_ddl_compiler:mk_helper_m2(DDL) of
		   {module, Module}  ->
		       Result = Module:validate_obj(Val),
		       GotPK = riak_ql_ddl:get_partition_key(DDL, Val),
		       GotLK = riak_ql_ddl:get_local_key(DDL, Val),
		       Expected = {?VALID, ExpectedPK, ExpectedLK},
		       Got = {Result, GotPK, GotLK},
		       ?assertEqual(Expected, Got);
		   _Other ->
		       ?debugFmt("~n~p compilation failed:~n~p", [Name, _Other]),
		       ?assert(false)
	       end).

-define(passing_short_test(Name, Query, Val), 
	Name() ->
	       Lexed = riak_ql_lexer:get_tokens(Query),
	       {ok, DDL} = riak_ql_parser:parse(Lexed),
	       case riak_ql_ddl_compiler:mk_helper_m2(DDL) of
		   {module, Module}  ->
		       Result = Module:validate_obj(Val),
		       ?assertEqual(?VALID, Result);
		   _Other ->
		       ?debugFmt("~n~p compilation failed:~n~p", [Name, _Other]),
		       ?assert(false)
	       end).


%%
%% this test calls into the PRIVATE interface
%% mk_helper_m2/1
%%
-define(failing_test(Name, Query, Val), 
	Name() ->
	       Lexed = riak_ql_lexer:get_tokens(Query),
	       {ok, DDL} = riak_ql_parser:parse(Lexed),
	       case riak_ql_ddl_compiler:mk_helper_m2(DDL) of
		   {module, Module}  ->
		       Result = Module:validate_obj(Val),
		       ?assertEqual(?INVALID, Result);
		   _Other ->
		       ?assertEqual(?INVALID, false) % didn't compile
	       end).


%%
%% this test calls in the PUBLIC interface
%% make_helper_mod/1
%%
-define(not_valid_test(Name, Query), 
	Name() ->
	       Lexed = riak_ql_lexer:get_tokens(Query),
	       {ok, DDL} = riak_ql_parser:parse(Lexed),
	       case riak_ql_ddl_compiler:make_helper_mod(DDL) of
		   {error, _} ->
		       ?assertEqual(?VALID, true);
		   Other ->
		       ?assertEqual(?VALID, Other) % didn't compile
	       end).

?passing_test(round_trip_test,
	      "create table temperatures " ++
		  "(time timestamp not null, " ++
		  "user_id varchar not null, " ++
		  "primary key (time, user_id))",
	      {12345, <<"beeees">>},
	      [{timestamp, 12345}, {binary, <<"beeees">>}],
	      [{timestamp, 12345}, {binary, <<"beeees">>}]).

?passing_short_test(integer_type_test,
		    "create table temperatures "
		    "(counter int not null, "
		    "primary key (counter))",
	      {12345}).

?passing_short_test(float_test,
		    "create table temperatures "
		    "(real_counter float not null, "
		    "primary key (real_counter))",
	      {12345.6}).

?failing_test(round_trip_fail_test,
	      "create table temperatures " ++
		  "(time timestamp not null, " ++
		  "user_id varchar not null, " ++
		  "primary key (time, user_id))",
	      {<<"banjette">>, <<"beeees">>}).

?passing_test(no_partition_key_test,
	      "create table temperatures " ++
		  "(time timestamp not null, " ++
		  "user_id varchar not null, " ++
		  "primary key (time, user_id))",
	      {12345, <<"beeees">>},
	      [{timestamp, 12345}, {binary, <<"beeees">>}],
	      [{timestamp, 12345}, {binary, <<"beeees">>}]).
	    
