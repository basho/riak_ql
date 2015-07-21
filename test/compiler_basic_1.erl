-module(compiler_basic_1).

-include_lib("eunit/include/eunit.hrl").

%% this is a basic test of timeseries that writes a single element to the back end
%% and checks it is correct

-define(VALID,   true).
-define(INVALID, false).

-compile(export_all).

-define(passing_test(Name, Query, Val), 
	Name() ->
	       Lexed = riak_ql_lexer:get_tokens(Query),
	       {ok, DDL} = riak_ql_parser:parse(Lexed),
	       case  riak_ql_ddl_compiler:make_helper_mod(DDL) of
		   {module, Module}  ->
		       Result = Module:validate_obj(Val),
		       ?assertEqual(?VALID, Result);
		   _Other ->
		       ?assertEqual(?INVALID, 'didnt compile')
	       end).

-define(failing_test(Name, Query, Val), 
	Name() ->
	       Lexed = riak_ql_lexer:get_tokens(Query),
	       {ok, DDL} = riak_ql_parser:parse(Lexed),
	       case  riak_ql_ddl_compiler:make_helper_mod(DDL) of
		   {module, Module}  ->
		       Result = Module:validate_obj(Val),
		       ?assertEqual(?INVALID, Result);
		   _Other ->
		       ?assertEqual(?INVALID, 'didnt compile')
	       end).


?passing_test(round_trip_test,
      "create table temperatures " ++
	  "(time timestamp not null, " ++
	  "user_id varchar not null, " ++
	  "primary key (time, user_id))",
      {12345, <<"beeees">>}).


?failing_test(round_trip_fail_test,
      "create table temperatures " ++
	  "(time timestamp not null, " ++
	  "user_id varchar not null, " ++
	  "primary key (time, user_id))",
      {<<"banjette">>, <<"beeees">>}).
    
