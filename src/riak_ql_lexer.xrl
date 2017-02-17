%%% -*- mode: erlang -*-
%%% @doc       Lexer for the riak Time Series Query Language.
%%% @author    gguthrie@basho.com
%%% @copyright (C) 2016 Basho

Definitions.

AND = (A|a)(N|n)(D|d)
ASC = (A|a)(S|s)(C|c)
BLOB = (B|b)(L|l)(O|o)(B|b)
BOOLEAN = (B|b)(O|o)(O|o)(L|l)(E|e)(A|a)(N|n)
BY = (B|b)(Y|y)
CREATE = (C|c)(R|r)(E|e)(A|a)(T|t)(E|e)
DELETE = (D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
DESC = (D|d)(E|e)(S|s)(C|c)
DESCRIBE = (D|d)(E|e)(S|s)(C|c)(R|r)(I|i)(B|b)(E|e)
DOUBLE = (D|d)(O|o)(U|u)(B|b)(L|l)(E|e)
EXPLAIN = (E|e)(X|x)(P|p)(L|l)(A|a)(I|i)(N|n)
FALSE = (F|f)(A|a)(L|l)(S|s)(E|e)
FIRST = (F|f)(I|i)(R|r)(S|s)(T|t)
FROM = (F|f)(R|r)(O|o)(M|m)
GROUP = (G|g)(R|r)(O|o)(U|u)(P|p)
KEY = (K|k)(E|e)(Y|y)
INSERT = (I|i)(N|n)(S|s)(E|e)(R|r)(T|t)
INTO = (I|i)(N|n)(T|t)(O|o)
LAST = (L|l)(A|a)(S|s)(T|t)
LIMIT = (L|l)(I|i)(M|m)(I|i)(T|t)
NOT = (N|n)(O|o)(T|t)
IS = (I|i)(S|s)
NULL = (N|n)(U|u)(L|l)(L|l)
NULLS = (N|n)(U|u)(L|l)(L|l)(S|s)
OFFSET = (O|o)(F|f)(F|f)(S|s)(E|e)(T|t)
OR = (O|o)(R|r)
ORDER = (O|o)(R|r)(D|d)(E|e)(R|r)
PRIMARY = (P|p)(R|r)(I|i)(M|m)(A|a)(R|r)(Y|y)
QUANTUM = (Q|q)(U|u)(A|a)(N|n)(T|t)(U|u)(M|m)
SELECT = (S|s)(E|e)(L|l)(E|e)(C|c)(T|t)
SHOW = (S|s)(H|h)(O|o)(W|w)
SINT64 = (S|s)(I|i)(N|n)(T|t)64
TABLE = (T|t)(A|a)(B|b)(L|l)(E|e)
TABLES = (T|t)(A|a)(B|b)(L|l)(E|e)(S|s)
TIMESTAMP = (T|t)(I|i)(M|m)(E|e)(S|s)(T|t)(A|a)(M|m)(P|p)
TRUE = (T|t)(R|r)(U|u)(E|e)
VALUES = (V|v)(A|a)(L|l)(U|u)(E|e)(S|s)
VARCHAR = (V|v)(A|a)(R|r)(C|c)(H|h)(A|a)(R|r)
WHERE = (W|w)(H|h)(E|e)(R|r)(E|e)
WITH = (W|w)(I|i)(T|t)(H|h)

CHARACTER_LITERAL = '(''|[^'\n])*'
HEX = 0x([0-9a-zA-Z]*)

REGEX = (/[^/][a-zA-Z0-9\*\.]+/i?)

IDENTIFIER = ([a-zA-Z][a-zA-Z0-9_\-]*)
QUOTED_IDENTIFIER = \"(\"\"|[^\"\n])*\"
COMMENT_MULTILINE = (/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(--.*)
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

PLUS     = (\+)
MINUS    = (\-)
ASTERISK = (\*)
SOLIDUS  = (/)

COMMA = (,)
SEMICOLON = (\;)

Rules.

{AND} : {token, {and_, list_to_binary(TokenChars)}}.
{ASC} : {token, {asc, list_to_binary(TokenChars)}}.
{BLOB} : {token, {blob, list_to_binary(TokenChars)}}.
{BOOLEAN} : {token, {boolean, list_to_binary(TokenChars)}}.
{BY} : {token, {by, list_to_binary(TokenChars)}}.
{CREATE} : {token, {create, list_to_binary(TokenChars)}}.
{DELETE} : {token, {delete, list_to_binary(TokenChars)}}.
{DESC} : {token, {desc, list_to_binary(TokenChars)}}.
{DESCRIBE} : {token, {describe, list_to_binary(TokenChars)}}.
{DOUBLE} : {token, {double, list_to_binary(TokenChars)}}.
{EXPLAIN} : {token, {explain, list_to_binary(TokenChars)}}.
{FALSE} : {token, {false, list_to_binary(TokenChars)}}.
{FIRST} : {token, {first, list_to_binary(TokenChars)}}.
{FROM} : {token, {from, list_to_binary(TokenChars)}}.
{INSERT} : {token, {insert, list_to_binary(TokenChars)}}.
{INTO} : {token, {into, list_to_binary(TokenChars)}}.
{GROUP} : {token, {group, list_to_binary(TokenChars)}}.
{GROUP_TIME} : {token, {group_time, list_to_binary(TokenChars)}}.
{KEY} : {token, {key, list_to_binary(TokenChars)}}.
{LAST} : {token, {last, list_to_binary(TokenChars)}}.
{LIMIT} : {token, {limit, list_to_binary(TokenChars)}}.
{NOT} : {token, {not_, list_to_binary(TokenChars)}}.
{NULL} : {token, {null, list_to_binary(TokenChars)}}.
{NULLS} : {token, {nulls, list_to_binary(TokenChars)}}.
{OFFSET} : {token, {offset, list_to_binary(TokenChars)}}.
{OR} : {token, {or_, list_to_binary(TokenChars)}}.
{ORDER} : {token, {order, list_to_binary(TokenChars)}}.
{PRIMARY} : {token, {primary, list_to_binary(TokenChars)}}.
{QUANTUM} : {token, {quantum, list_to_binary(TokenChars)}}.
{SELECT} : {token, {select, list_to_binary(TokenChars)}}.
{SHOW} : {token, {show, list_to_binary(TokenChars)}}.
{SINT64} : {token, {sint64, list_to_binary(TokenChars)}}.
{TABLE} : {token, {table, list_to_binary(TokenChars)}}.
{TABLES} : {token, {tables, list_to_binary(TokenChars)}}.
{TIMESTAMP} : {token, {timestamp, list_to_binary(TokenChars)}}.
{TRUE} : {token, {true, list_to_binary(TokenChars)}}.
{VALUES} : {token, {values, list_to_binary(TokenChars)}}.
{VARCHAR} : {token, {varchar, list_to_binary(TokenChars)}}.
{WHERE} : {token, {where, list_to_binary(TokenChars)}}.
{WITH} : {token, {with, list_to_binary(TokenChars)}}.

{HEX} : {token, {character_literal, clean_up_hex(TokenChars)}}.
{INTNUM}   : {token, {integer, list_to_integer(TokenChars)}}.

% float chars do not get converted to floats, if they are part of a word
% then converting it and converting it back will alter the chars
{FLOATDEC} : {token, {float, TokenChars}}.
{FLOATSCI} : {token, {float_sci, TokenChars}}.

{IS}          : {token, {is_,                 list_to_binary(TokenChars)}}.
{EQ}          : {token, {equals_operator,     list_to_binary(TokenChars)}}.
{APPROXMATCH} : {token, {approx, list_to_binary(TokenChars)}}.
{GT}          : {token, {greater_than_operator, list_to_binary(TokenChars)}}.
{LT}          : {token, {less_than_operator,    list_to_binary(TokenChars)}}.
{GTE}         : {token, {gte,       list_to_binary(TokenChars)}}.
{LTE}         : {token, {lte,       list_to_binary(TokenChars)}}.
{NE}          : {token, {ne,        list_to_binary(TokenChars)}}.
{NOMATCH}     : {token, {nomatch,   list_to_binary(TokenChars)}}.
{NOTAPPROX}   : {token, {notapprox, list_to_binary(TokenChars)}}.

{OPEN}  :  {token, {left_paren,  list_to_binary(TokenChars)}}.
{CLOSE} :  {token, {right_paren, list_to_binary(TokenChars)}}.

{PLUS}     : {token, {plus_sign,  list_to_binary(TokenChars)}}.
{MINUS}    : {token, {minus_sign, list_to_binary(TokenChars)}}.
{ASTERISK} : {token, {asterisk,   list_to_binary(TokenChars)}}.
{SOLIDUS}  : {token, {solidus,    list_to_binary(TokenChars)}}.

{CHARACTER_LITERAL} :
  {token, {character_literal, clean_up_literal(TokenChars)}}.

{REGEX} : {token, {regex, list_to_binary(TokenChars)}}.

{COMMA} : {token, {comma, list_to_binary(TokenChars)}}.
{SEMICOLON} : {token, {semicolon, list_to_binary(TokenChars)}}.

{COMMENT_MULTILINE} : skip_token.
{WHITESPACE} : skip_token.

\n : {end_token, {'$end'}}.

{IDENTIFIER} : {token, {identifier, clean_up_identifier(TokenChars)}}.
{QUOTED_IDENTIFIER} : {token, {identifier, clean_up_identifier(TokenChars)}}.
{UNICODE} : error(unicode_in_identifier).

.  : error(iolist_to_binary(io_lib:format("Unexpected token '~s'.", [TokenChars]))).

Erlang code.

-compile([export_all]).

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
    post_p(T, [{float, fpdec_to_float(TokenChars)} | Acc]);
post_p([{float_sci, TokenChars} | T], Acc) ->
    post_p(T, [{float, fpsci_to_float(TokenChars)} | Acc]);
post_p([H | T], Acc) ->
    post_p(T, [H | Acc]).

lex(String) ->
    {ok, Toks, _} = string(String),
    Toks.

clean_up_identifier(Literal) ->
    clean_up_literal(Literal).

clean_up_hex([$0,$x|Hex]) ->
    case length(Hex) rem 2 of
        0 ->
            mochihex:to_bin(Hex);
        _ ->
            error({odd_hex_chars,<<"Hex strings must have an even number of characters.">>})
    end.

clean_up_literal(Literal) ->
    Literal1 = case hd(Literal) of
        $' -> accurate_strip(Literal, $');
        $" ->
            [error(unicode_in_quotes) || U <- Literal, U > 127],
            accurate_strip(Literal, $");
        _ -> Literal
    end,
    DeDupedInternalQuotes = dedup_quotes(Literal1),
    list_to_binary(DeDupedInternalQuotes).

%% dedup(licate) quotes, using pattern matching to reduce to O(n)
dedup_quotes(S) ->
    dedup_quotes(S, []).
dedup_quotes([], Acc) ->
    lists:reverse(Acc);
dedup_quotes([H0,H1|T], Acc) when H0 =:= $' andalso H1 =:= $' ->
    dedup_quotes(T, [H0|Acc]);
dedup_quotes([H0,H1|T], Acc) when H0 =:= $" andalso H1 =:= $" ->
    dedup_quotes(T, [H0|Acc]);
dedup_quotes([H|T], Acc) ->
    dedup_quotes(T, [H|Acc]).

%% only strip one quote, to accept Literals ending in the quote
%% character being stripped
accurate_strip(S, C) ->
    case {hd(S), lists:last(S), length(S)} of
        {C, C, Len} when Len > 1 ->
            string:substr(S, 2, Len - 2);
        _ ->
            S
    end.

fpsci_to_float(Chars) ->
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

fpdec_to_float([$- | _RemainTokenChars] = TokenChars) ->
    list_to_float(TokenChars);
fpdec_to_float([$. | _RemainTokenChars] = TokenChars) ->
    list_to_float([$0 | TokenChars]);
fpdec_to_float(TokenChars) ->
    list_to_float(TokenChars).
