%%% -*- mode: erlang -*-
%%% @doc       Lexer for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2015 Basho

Definitions.

AND = (A|a)(N|n)(D|d)
AS = (A|a)(S|s)
ATOM = (A|a)(T|t)(O|o)(M|m)
CREATE_TABLE = (C|c)(R|r)(E|e)(A|a)(T|t)(E|e)\s(T|t)(A|a)(B|b)(L|l)(E|e)
DELETE = (D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
DROP = (D|d)(R|r)(O|o)(P|p)
FLOAT = (F|f)(L|l)(O|o)(A|a)(T|t)
FROM = (F|f)(R|r)(O|o)(M|m)
GLOBAL = (G|g)(L|l)(O|o)(B|b)(A|a)(L|l)
GROUPBY = (G|g)(R|r)(O|o)(U|u)(P|p)(B|b)(Y|y)
INNER = (I|i)(N|n)(N|n)(E|e)(R|r)
INT = (I|i)(N|n)(T|t)
JOIN = (J|j)(O|o)(I|i)(N|n)
LIMIT = (L|l)(I|i)(M|m)(I|i)(T|t)
LOCAL = (L|l)(O|o)(C|c)(A|a)(L|l)
LOCAL_KEY = (L|l)(O|o)(C|c)(A|a)(L|l)\s(K|k)(E|e)(Y|y)
MERGE = (M|m)(E|e)(R|r)(G|g)(E|e)
MODFUN = (M|m)(O|o)(D|d)(F|f)(U|u)(N|n)
NOT_NULL = (N|n)(O|o)(T|t)\s(N|n)(U|u)(L|l)(L|l)
OF = (O|o)(F|f)
ON_COMMIT = (O|o)(N|n)\s(C|c)(O|o)(M|m)(M|m)(I|i)(T|t)
OR = (O|o)(R|r)
PRIMARY_KEY = (P|p)(R|r)(I|i)(M|m)(A|a)(R|r)(Y|y)\s(K|k)(E|e)(Y|y)
PRESERVE = (P|p)(R|r)(E|e)(S|s)(E|e)(R|r)(V|v)(E|e)
QUANTUM = (Q|q)(U|u)(A|a)(N|n)(T|t)(U|u)(M|m)
ROWS = (R|r)(O|o)(W|w)(S|s)
SELECT = (S|s)(E|e)(L|l)(E|e)(C|c)(T|t)
SYSTEM_VERSIONING = (S|s)(Y|y)(S|s)(T|t)(E|e)(M|m)\s(V|v)(E|e)(R|r)(S|s)(I|i)(O|o)(N|n)(I|i)(N|n)(G|g)
TEMPORARY = (T|t)(E|e)(M|m)(P|p)(O|o)(R|r)(A|a)(R|r)(Y|y)
TIMESTAMP = (T|t)(I|i)(M|m)(E|e)(S|s)(T|t)(A|a)(M|m)(P|p)
VARCHAR = (V|v)(A|a)(R|r)(C|c)(H|h)(A|a)(R|r)
WHERE = (W|w)(H|h)(E|e)(R|r)(E|e)
WITH = (W|w)(I|i)(T|t)(H|h)

DATETIME = ('[0-9a-zA-Z\s:\-\.]*')

REGEX = (/[^/][a-zA-Z0-9\*\.]+/i?)

QUOTED = ("(.*(\")*)")

INTERP = (:[a-zA-Z][0-9a-zA-Z_]*)

WHITESPACE = ([\000-\s]*)

INTNUM   = (\-*[0-9]+)
FLOATDEC = (\-*([0-9]+)?\.[0-9]+)
FLOATSCI = (\-*([0-9]+)?(\.)?[0-9]+(E|e)(\+|\-)?[0-9]+)

APPROXMATCH = (=\~)
EQ          = (=)
GT          = (>)
LT          = (<)
NE          = (<>)
NOMATCH     = (!=)
NOTAPPROX   = (!\~)
OPEN        = \(
CLOSE       =\)

PLUS  = (\+)
MINUS = (\-)
TIMES = (\*)
DIV   = (/)

COMMA = (,)

Rules.

{AND} : {token, {and_, list_to_binary(TokenChars)}}.
{AS} : {token, {as, list_to_binary(TokenChars)}}.
{ATOM} : {token, {atom, list_to_binary(TokenChars)}}.
{CREATE_TABLE} : {token, {create_table, list_to_binary(TokenChars)}}.
{DELETE} : {token, {delete, list_to_binary(TokenChars)}}.
{DROP} : {token, {drop, list_to_binary(TokenChars)}}.
{FLOAT} : {token, {float_type, list_to_binary(TokenChars)}}.
{FROM} : {token, {from, list_to_binary(TokenChars)}}.
{GLOBAL} : {token, {global, list_to_binary(TokenChars)}}.
{GROUPBY} : {token, {groupby, list_to_binary(TokenChars)}}.
{INNER} : {token, {inner, list_to_binary(TokenChars)}}.
{INT} : {token, {int_type, list_to_binary(TokenChars)}}.
{JOIN} : {token, {join, list_to_binary(TokenChars)}}.
{LIMIT} : {token, {limit, list_to_binary(TokenChars)}}.
{LOCAL} : {token, {local, list_to_binary(TokenChars)}}.
{LOCAL_KEY} : {token, {local_key, list_to_binary(TokenChars)}}.
{MERGE} : {token, {merge, list_to_binary(TokenChars)}}.
{MODFUN} : {token, {modfun, list_to_binary(TokenChars)}}.
{NOT_NULL} : {token, {not_null, list_to_binary(TokenChars)}}.
{OF} : {token, {of_, list_to_binary(TokenChars)}}.
{ON_COMMIT} : {token, {on_commit, list_to_binary(TokenChars)}}.
{OR} : {token, {or_, list_to_binary(TokenChars)}}.
{PRIMARY_KEY} : {token, {primary_key, list_to_binary(TokenChars)}}.
{PRESERVE} : {token, {preserve, list_to_binary(TokenChars)}}.
{QUANTUM} : {token, {quantum, list_to_binary(TokenChars)}}.
{ROWS} : {token, {rows, list_to_binary(TokenChars)}}.
{SELECT} : {token, {select, list_to_binary(TokenChars)}}.
{SYSTEM_VERSIONING} : {token, {system_versioning, list_to_binary(TokenChars)}}.
{TEMPORARY} : {token, {temporary, list_to_binary(TokenChars)}}.
{TIMESTAMP} : {token, {timestamp, list_to_binary(TokenChars)}}.
{VARCHAR} : {token, {varchar, list_to_binary(TokenChars)}}.
{WHERE} : {token, {where, list_to_binary(TokenChars)}}.
{WITH} : {token, {with, list_to_binary(TokenChars)}}.

{INTNUM}   : {token, {int, list_to_integer(TokenChars)}}.
{FLOATDEC} : {token, {float, list_to_float(TokenChars)}}.
{FLOATSCI} : {token, {float, list_to_float(TokenChars)}}.

{EQ}          : {token, {eq,     list_to_binary(TokenChars)}}.
{APPROXMATCH} : {token, {approx, list_to_binary(TokenChars)}}.
{GT}          : {token, {gt,        list_to_binary(TokenChars)}}.
{LT}          : {token, {lt,        list_to_binary(TokenChars)}}.
{NE}          : {token, {ne,        list_to_binary(TokenChars)}}.
{NOMATCH}     : {token, {nomatch,   list_to_binary(TokenChars)}}.
{NOTAPPROX}   : {token, {notapprox,   list_to_binary(TokenChars)}}.

{OPEN}  :  {token, {openb,  list_to_binary(TokenChars)}}.
{CLOSE} :  {token, {closeb, list_to_binary(TokenChars)}}.

{PLUS}  : {token, {plus,       list_to_binary(TokenChars)}}.
{MINUS} : {token, {minus,      list_to_binary(TokenChars)}}.
{TIMES} : {token, {maybetimes, list_to_binary(TokenChars)}}.
{DIV}   : {token, {div_,       list_to_binary(TokenChars)}}.

{DATETIME} : {token, fix_up_date(TokenChars)}.
{INTERP} : {token, fix_up_interp(TokenChars)}.

{QUOTED} : {token, {quoted, strip_quoted(TokenChars)}}.

{REGEX} : {token, {regex, list_to_binary(TokenChars)}}.

{COMMA} : {token, {comma, list_to_binary(TokenChars)}}.

{WHITESPACE} : {token, {whitespace, list_to_binary(TokenChars)}}.

\n : {end_token, {'$end'}}.

.  : {token, {chars, list_to_binary(TokenChars)}}.

Erlang code.

-export([
         get_tokens/1
        ]).

-include("riak_ql.xrl.tests").

get_tokens(X) ->
    Toks = lex(X),
    post_process(Toks).

post_process(X) ->
    post_p(X, []).

% filter out the whitespaces at the end
post_p([], Acc) ->
    [{Type, X} || {Type, X} <- lists:reverse(Acc), Type =/= whitespace];
% when you've merged two items hoy them back on the list
% so they can continue to sook up chars
post_p([{Word1, W1}, {Word2, W2} | T], Acc) when Word1 =:= chars   orelse
                                                 Word1 =:= select  orelse
                                                 Word1 =:= from    orelse
                                                 Word1 =:= limit   orelse
                                                 Word1 =:= and_    orelse
                                                 Word1 =:= or_     orelse
                                                 Word1 =:= delete  orelse
                                                 Word1 =:= drop    orelse
                                                 Word1 =:= groupby orelse
                                                 Word1 =:= merge   orelse
                                                 Word1 =:= inner   orelse
                                                 Word1 =:= join    orelse
                                                 Word1 =:= as,
                                                 Word2 =:= chars   orelse
                                                 Word2 =:= select  orelse
                                                 Word2 =:= from    orelse
                                                 Word2 =:= limit   orelse
                                                 Word2 =:= and_    orelse
                                                 Word2 =:= or_     orelse
                                                 Word2 =:= delete  orelse
                                                 Word2 =:= drop    orelse
                                                 Word2 =:= groupby orelse
                                                 Word2 =:= merge   orelse
                                                 Word2 =:= inner   orelse
                                                 Word2 =:= join    orelse
                                                 Word2 =:= as      ->
    post_p([{chars, <<W1/binary, W2/binary>>} | T], Acc);
post_p([{chars, TokenChars} | T], Acc) when is_list(TokenChars)->
    post_p(T, [{chars, list_to_binary(TokenChars)} | Acc]);
post_p([H | T], Acc) ->
    post_p(T, [H | Acc]).

lex(String) ->
    {ok, Toks, _} = string(String),
    Toks.

fix_up_date(Date) ->
    Date2 = string:strip(Date, both, $'), %'
    Date3 = string:strip(Date2),
    case dh_date:parse(Date3) of
        {error, bad_date} -> {chars, Date2};
        Date4             -> {datetime, Date4}
    end.

fix_up_interp([$: | InterpName]) ->
    {interp, InterpName}.

strip_quoted(Date) ->
    Date2 = string:strip(Date, both, $"), %"
    list_to_binary(string:strip(Date2)).
