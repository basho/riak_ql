%%% -*- mode: erlang -*-
%%% @doc       Lexer for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2015 Basho

Definitions.

AND = (A|a)(N|n)(D|d)
ASC = (A|a)(S|s)(C|c)
BOOLEAN = (B|b)(O|o)(O|o)(L|l)(E|e)(A|a)(N|n)
CREATE = (C|c)(R|r)(E|e)(A|a)(T|t)(E|e)
DESC = (D|d)(E|e)(S|s)(C|c)
DOUBLE = (D|d)(O|o)(U|u)(B|b)(L|l)(E|e)
FALSE = (F|f)(A|a)(L|l)(S|s)(E|e)
FROM = (F|f)(R|r)(O|o)(M|m)
KEY = (K|k)(E|e)(Y|y)
LIMIT = (L|l)(I|i)(M|m)(I|i)(T|t)
NOT = (N|n)(O|o)(T|t)
NULL = (N|n)(U|u)(L|l)(L|l)
OR = (O|o)(R|r)
PRIMARY = (P|p)(R|r)(I|i)(M|m)(A|a)(R|r)(Y|y)
QUANTUM = (Q|q)(U|u)(A|a)(N|n)(T|t)(U|u)(M|m)
SELECT = (S|s)(E|e)(L|l)(E|e)(C|c)(T|t)
SINT64 = (S|s)(I|i)(N|n)(T|t)64
TABLE = (T|t)(A|a)(B|b)(L|l)(E|e)
TIMESTAMP = (T|t)(I|i)(M|m)(E|e)(S|s)(T|t)(A|a)(M|m)(P|p)
TRUE = (T|t)(R|r)(U|u)(E|e)
VARCHAR = (V|v)(A|a)(R|r)(C|c)(H|h)(A|a)(R|r)
WHERE = (W|w)(H|h)(E|e)(R|r)(E|e)

CHARACTER_LITERAL = ('([^\']|(\'\'))*')

REGEX = (/[^/][a-zA-Z0-9\*\.]+/i?)

QUOTED = ("([^\"]|(\"\"))*")
IDENTIFIER = ([a-zA-Z][a-zA-Z0-9_\-]*)
WHITESPACE = ([\000-\s]*)

% characters not in the ascii range
UNICODE = ([^\x00-\x7F])

INTNUM   = (\-*[0-9]+)
FLOATDEC = (\-*([0-9]+)?\.[0-9]+)
FLOATSCI = (\-*([0-9]+)?(\.)?[0-9]+(E|e)(\+|\-)?[0-9]+)

APPROXMATCH = (=\~)
EQ          = (=)
GT          = (>)
LT          = (<)
GTE         = (>=)
LTE         = (<=)
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
{ASC} : {token, {asc, list_to_binary(TokenChars)}}.
{BOOLEAN} : {token, {boolean, list_to_binary(TokenChars)}}.
{CREATE} : {token, {create, list_to_binary(TokenChars)}}.
{DESC} : {token, {desc, list_to_binary(TokenChars)}}.
{DOUBLE} : {token, {double, list_to_binary(TokenChars)}}.
{FALSE} : {token, {false, list_to_binary(TokenChars)}}.
{FROM} : {token, {from, list_to_binary(TokenChars)}}.
{KEY} : {token, {key, list_to_binary(TokenChars)}}.
{LIMIT} : {token, {limit, list_to_binary(TokenChars)}}.
{NOT} : {token, {not_, list_to_binary(TokenChars)}}.
{NULL} : {token, {null, list_to_binary(TokenChars)}}.
{OR} : {token, {or_, list_to_binary(TokenChars)}}.
{PRIMARY} : {token, {primary, list_to_binary(TokenChars)}}.
{QUANTUM} : {token, {quantum, list_to_binary(TokenChars)}}.
{SELECT} : {token, {select, list_to_binary(TokenChars)}}.
{SINT64} : {token, {sint64, list_to_binary(TokenChars)}}.
{TABLE} : {token, {table, list_to_binary(TokenChars)}}.
{TIMESTAMP} : {token, {timestamp, list_to_binary(TokenChars)}}.
{TRUE} : {token, {true, list_to_binary(TokenChars)}}.
{VARCHAR} : {token, {varchar, list_to_binary(TokenChars)}}.
{WHERE} : {token, {where, list_to_binary(TokenChars)}}.

{INTNUM}   : {token, {integer, list_to_integer(TokenChars)}}.

% float chars do not get converted to floats, if they are part of a word
% then converting it and converting it back will alter the chars
{FLOATDEC} : {token, {float, TokenChars}}.
{FLOATSCI} : {token, {float_sci, TokenChars}}.

{EQ}          : {token, {eq,     list_to_binary(TokenChars)}}.
{APPROXMATCH} : {token, {approx, list_to_binary(TokenChars)}}.
{GT}          : {token, {gt,        list_to_binary(TokenChars)}}.
{LT}          : {token, {lt,        list_to_binary(TokenChars)}}.
{GTE}         : {token, {gte,       list_to_binary(TokenChars)}}.
{LTE}         : {token, {lte,       list_to_binary(TokenChars)}}.
{NE}          : {token, {ne,        list_to_binary(TokenChars)}}.
{NOMATCH}     : {token, {nomatch,   list_to_binary(TokenChars)}}.
{NOTAPPROX}   : {token, {notapprox, list_to_binary(TokenChars)}}.

{OPEN}  :  {token, {openb,  list_to_binary(TokenChars)}}.
{CLOSE} :  {token, {closeb, list_to_binary(TokenChars)}}.

{PLUS}  : {token, {plus,       list_to_binary(TokenChars)}}.
{MINUS} : {token, {minus,      list_to_binary(TokenChars)}}.
{TIMES} : {token, {maybetimes, list_to_binary(TokenChars)}}.
{DIV}   : {token, {div_,       list_to_binary(TokenChars)}}.

{CHARACTER_LITERAL} :
  {token, {character_literal, clean_up_literal(TokenChars)}}.

{QUOTED} : {token, {identifier, strip_quoted(TokenChars)}}.

{REGEX} : {token, {regex, list_to_binary(TokenChars)}}.

{COMMA} : {token, {comma, list_to_binary(TokenChars)}}.

{WHITESPACE} : skip_token.

\n : {end_token, {'$end'}}.

{IDENTIFIER} : {token, {identifier, list_to_binary(TokenChars)}}.
{UNICODE} : error(unicode_in_identifier).

.  : error(iolist_to_binary(io_lib:format("Unexpected token '~s'.", [TokenChars]))).

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

%% filter out the whitespaces at the end
post_p([], Acc) ->
     lists:reverse(Acc);
post_p([{identifier, TokenChars} | T], Acc) when is_list(TokenChars)->
    post_p(T, [{identifier, list_to_binary(TokenChars)} | Acc]);
post_p([{float, TokenChars} | T], Acc) ->
    post_p(T, [{float, parse_float(TokenChars)} | Acc]);
post_p([{float_sci, TokenChars} | T], Acc) ->
    post_p(T, [{float, sci_to_float(TokenChars)} | Acc]);
post_p([H | T], Acc) ->
    post_p(T, [H | Acc]).

lex(String) ->
    {ok, Toks, _} = string(String),
    Toks.

clean_up_literal(Literal) ->
    RemovedOutsideQuotes = string:strip(Literal, both, $'), %'
    DeDoubledInternalQuotes = re:replace(RemovedOutsideQuotes,
                                         "''", "'",
                                         [global, {return, list}]),
    list_to_binary(DeDoubledInternalQuotes).

strip_quoted(QuotedString) ->
    % if there are unicode characters in the string, throw an error
    [error(unicode_in_quotes) || U <- QuotedString, U > 127],

    StrippedOutsideQuotes = string:strip(QuotedString, both, $"),
    re:replace(StrippedOutsideQuotes, "\"\"", "\"", [global, {return, binary}]).

sci_to_float(Chars) ->
    [Mantissa, Exponent] = re:split(Chars, "E|e", [{return, list}]),
    M2 = normalise_mant(Mantissa),
    E2 = normalise_exp(Exponent),
    sci_to_f2(M2, E2).

sci_to_f2(M2, E) when E =:= "+0" orelse
                      E =:= "-0" -> list_to_float(M2);
sci_to_f2(M2, E2) -> list_to_float(M2 ++ "e" ++ E2).

normalise_mant(Mantissa) ->
    case length(re:split(Mantissa, "\\.", [{return, list}])) of
        1 -> Mantissa ++ ".0";
        2 -> Mantissa
    end.

normalise_exp("+" ++ No) -> "+" ++ No;
normalise_exp("-" ++ No) -> "-" ++ No;
normalise_exp(No)        -> "+" ++ No.

parse_float([$- | _RemainTokenChars] = TokenChars) ->
    list_to_float(TokenChars);
parse_float([$. | _RemainTokenChars] = TokenChars) ->
    list_to_float([$0 | TokenChars]);
parse_float(TokenChars) ->
    list_to_float(TokenChars).
