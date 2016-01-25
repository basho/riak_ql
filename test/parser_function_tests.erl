-module(parser_function_tests).

-include_lib("eunit/include/eunit.hrl").
-include("parser_test_utils.hrl").

function_arity_0_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun() = a"))
      ).

function_identifier_arity_1_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun(a) = a"))
      ).

function_identifier_arity_2_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun(a, b) = a"))
      ).

function_val_arity_1_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a') = a"))
      ).

function_val_arity_2_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 'b') = a"))
      ).

function_val_arity_3_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 'b', 'c') = a"))
      ).

function_val_arity_10_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b') = a"))
      ).

function_val_and_identifier_mix_1_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 10, b, 3.5) = a"))
      ).

function_val_and_identifier_mix_2_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 10, b, 3.5, true) = a"))
      ).

function_val_and_identifier_mix_3_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<_/binary>>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a', 10, b, 3.5, false) = a"))
      ).

function_call_error_message_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser, <<"Function not supported - 'myfun'.">>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun('a') = a"))
      ).

function_as_arg_test() ->
    ?assertMatch(
       {error, {0, riak_ql_parser,
                <<"Function not supported - 'herfun'.">>}},
       riak_ql_parser:parse(riak_ql_lexer:get_tokens("select f from a WHERE myfun(hisfun(herfun(a))) = 'a'"))
      ).
