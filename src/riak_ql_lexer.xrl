%%% -*- mode: erlang -*-
%%% @doc       Lexer for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2015 Basho

Definitions.

SELECT  = (S|s)(E|e)(L|l)(E|e)(C|c)(T|t)
FROM    = (F|f)(R|r)(O|o)(M|m)
LIMIT   = (L|l)(I|i)(M|m)(I|i)(T|t)
WHERE   = (W|w)(H|h)(E|e)(R|r)(E|e)
AND     = (A|a)(N|n)(D|d)
OR      = (O|o)(R|r)
DELETE  = (D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
DROP    = (D|d)(R|r)(O|o)(P|p)
GROUPBY = (G|g)(R|r)(O|o)(U|u)(P|p)(B|b)(Y|y)
MERGE   = (M|m)(E|e)(R|r)(G|g)(E|e)
INNER   = (I|i)(N|n)(N|n)(E|e)(R|r)
JOIN    = (J|j)(O|o)(I|i)(N|n)
AS      = (A|a)(S|s)

DATETIME = ('[0-9a-zA-Z\s:\-\.]*')

REGEX = (/[^/][a-zA-Z0-9\*\.]+/i?)

QUOTED = ("(.*(\")*)")

WHITESPACE = ([\000-\s]*)

INT      = (\-*[0-9]+)
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

{SELECT}  : {token, {select,  TokenChars}}.
{FROM}    : {token, {from,    TokenChars}}.
{LIMIT}   : {token, {limit,   TokenChars}}.
{WHERE}   : {token, {where,   TokenChars}}.
{AND}     : {token, {and_,    TokenChars}}.
{OR}      : {token, {or_,     TokenChars}}.
{DELETE}  : {token, {delete,  TokenChars}}.
{DROP}    : {token, {drop,    TokenChars}}.
{GROUPBY} : {token, {groupby, TokenChars}}.
{MERGE}   : {token, {merge,   TokenChars}}.
{INNER}   : {token, {inner,   TokenChars}}.
{JOIN}    : {token, {join,    TokenChars}}.
{AS}      : {token, {as,      TokenChars}}.

{INT}      : {token, {int,   list_to_integer(TokenChars)}}.
{FLOATDEC} : {token, {float, list_to_float(TokenChars)}}.
{FLOATSCI} : {token, {float, list_to_float(TokenChars)}}.

{EQ}          : {token, {eq,     TokenChars}}.
{APPROXMATCH} : {token, {approx, TokenChars}}.
{GT}          : {token, {gt,        TokenChars}}.
{LT}          : {token, {lt,        TokenChars}}.
{NE}          : {token, {ne,        TokenChars}}.
{NOMATCH}     : {token, {nomatch,   TokenChars}}.
{NOTAPPROX}   : {token, {notapprox,   TokenChars}}.

{OPEN}  :  {token, {openb,  TokenChars}}.
{CLOSE} :  {token, {closeb, TokenChars}}.

{PLUS}  : {token, {plus,       TokenChars}}.
{MINUS} : {token, {minus,      TokenChars}}.
{TIMES} : {token, {maybetimes, TokenChars}}.
{DIV}   : {token, {div_,       TokenChars}}.

{DATETIME} : {token, fix_up_date(TokenChars)}.

{QUOTED} : {token, {quoted, strip_quoted(TokenChars)}}.

{REGEX} : {token, {regex, TokenChars}}.

{COMMA} : {token, {comma, TokenChars}}.

{WHITESPACE} : {token, {whitespace, TokenChars}}.

\n : {end_token, {'$end'}}.

.  : {token, {chars, TokenChars}}.

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
    post_p([{chars, W1 ++ W2} | T], Acc);
post_p([H | T], Acc) ->
    post_p(T, [H | Acc]).

lex(String) -> {ok, Toks, 1} = string(String),
               Toks.

fix_up_date(Date) ->
    Date2 = string:strip(Date, both, $'), %'
    Date3 = string:strip(Date2),
    case dh_date:parse(Date3) of
        {error, bad_date} -> {chars, Date2};
        Date4             -> {datetime, Date4}
    end.


strip_quoted(Date) ->
    Date2 = string:strip(Date, both, $"), %"
    string:strip(Date2).
