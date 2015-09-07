-module(lexer_basic_1).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

numbers_in_between_chars_test() ->
    ?assertEqual(
    	[{chars, <<"at45hi">>}],
    	riak_ql_lexer:get_tokens("at45hi")
	).

numbers_after_text_test() ->
    ?assertEqual(
    	[{chars, <<"hi5">>}],
    	riak_ql_lexer:get_tokens("hi5")
	).