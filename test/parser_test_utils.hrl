-include_lib("eunit/include/eunit.hrl").

-include("riak_ql_ddl.hrl").

-define(sql_comp_assert(String, Expected),
        Exp2 = fix(Expected),
        Toks = riak_ql_lexer:get_tokens(String),
        Got = riak_ql_parser:parse(Toks),
        ?assertEqual({ok, Exp2}, Got)).

-define(where_test(Uncanonical, Expected),
        Got = riak_ql_parser:canonicalise_where(Uncanonical),
        ?assertEqual(Expected, Got)).

%% assert match is useful for only matching part of the output, that
%% is being tested and not the whole state to prevent tests failing
%% on irrelevant changes.
-define(sql_comp_assert_match(String, Expected),
        Toks = riak_ql_lexer:get_tokens(String),
        Got = riak_ql_parser:parse(Toks),
        ?assertMatch({ok, Expected}, Got)).

-define(sql_comp_fail(QL_string),
        Toks = riak_ql_lexer:get_tokens(QL_string),
        Got = riak_ql_parser:parse(Toks),
        ?assertMatch({error, _}, Got)).

fix(?SQL_SELECT{'FROM' = F} = Expected) ->
    case F of
        {regex, _} -> Expected;
        {list,  _} -> Expected;
        _          -> Mod = riak_ql_ddl:make_module_name(F),
                      Expected?SQL_SELECT{helper_mod = Mod}
    end;
fix(Other) ->
    Other.
