-module(riak_ql_parser).
-export([parse/1, parse_and_scan/1, format_error/1]).
-file("src/riak_ql_parser.yrl", 123).

-compile(export_all).

-record(outputs,
	{
	  type    = [] :: select | drop | delete,
	  fields  = [], 
	  buckets = [],
	  limit   = none,
	  where   = none,
	  ops     = []
	 }).

-include("riak_ql.yrl.tests").

process({chars, A}) ->
    {word, A}.

concatenate({word, A}, {chars, B}) ->
    {word, A ++ B}.

make_clause(A, B, C, D) -> make_clause(A, B, C, D, {where, none}).
    
make_clause({select, A}, {_, B}, {from, _C}, {Type, D}, {_, E}) ->
    Type2 = case Type of
		list   -> list;
		word   -> string;
		quoted -> string;
		regex  -> regex
	    end,
    _O = #outputs{type    = list_to_existing_atom(A),
		  fields  = B,
		  buckets = {Type2, D},
		  where   = E
		 }.

log(A, Str) ->
    Msg = io_lib:format(Str ++ " ~p~n", [A]),
    bits:log_terms(lists:flatten(Msg)),
    A.

add_limit(A, _B, {int, C}) ->
    A#outputs{limit = C}.

make_expr({_, A}, {B, _}, {Type, C}) ->
    B1 = case B of
	     and_      -> and_;
	     or_       -> or_;
	     plus      -> '+';
	     minus     -> '-';
	     maybetime -> '*';
	     div_      -> '/';
	     gt        -> '>';
	     lt        -> '<';
	     eq        -> '=';
	     ne        -> '<>';
	     approx    -> '=~';
	     notapprox -> '!~';
	     nomatch   -> '!='
	 end,
    C2 = case Type of
	     conditional -> C;
	     _           -> {Type, C}
	 end,
    {conditional, {B1, {A, C2}}}.

make_where({where, A}, {conditional, B}) ->
    {A, B}.

make_funcall({A, B}) ->
     make_funcall({A, B}, []).

make_funcall({A, B}, C) ->
    Msg = io_lib:format("in make_funcall with ~p and ~p~n", [A, B]),
    bits:log_terms(lists:flatten(Msg)),
    {funcall, {B, C}}.

add_unit({Type, A}, {chars, U}) when U =:= "s" -> {Type, A};
add_unit({Type, A}, {chars, U}) when U =:= "m" -> {Type, A*60};
add_unit({Type, A}, {chars, U}) when U =:= "h" -> {Type, A*60*60};
add_unit({Type, A}, {chars, U}) when U =:= "d" -> {Type, A*60*60*24}.

make_list({list, A}, {_, B}) -> {list, A ++ [B]};
make_list({_,    A}, {_, B}) -> {list, [A, B]}.

make_expr(A) ->
    {conditional, A}.

-file("/usr/local/lib/erlang/lib/parsetools-2.0.10/include/yeccpre.hrl", 0).
%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2013. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% The parser generator will insert appropriate declarations before this line.%

-type yecc_ret() :: {'error', _} | {'ok', _}.

-spec parse(Tokens :: list()) -> yecc_ret().
parse(Tokens) ->
    yeccpars0(Tokens, {no_func, no_line}, 0, [], []).

-spec parse_and_scan({function() | {atom(), atom()}, [_]}
                     | {atom(), atom(), [_]}) -> yecc_ret().
parse_and_scan({F, A}) ->
    yeccpars0([], {{F, A}, no_line}, 0, [], []);
parse_and_scan({M, F, A}) ->
    Arity = length(A),
    yeccpars0([], {{fun M:F/Arity, A}, no_line}, 0, [], []).

-spec format_error(any()) -> [char() | list()].
format_error(Message) ->
    case io_lib:deep_char_list(Message) of
        true ->
            Message;
        _ ->
            io_lib:write(Message)
    end.

%% To be used in grammar files to throw an error message to the parser
%% toplevel. Doesn't have to be exported!
-compile({nowarn_unused_function, return_error/2}).
-spec return_error(integer(), any()) -> no_return().
return_error(Line, Message) ->
    throw({error, {Line, ?MODULE, Message}}).

-define(CODE_VERSION, "1.4").

yeccpars0(Tokens, Tzr, State, States, Vstack) ->
    try yeccpars1(Tokens, Tzr, State, States, Vstack)
    catch 
        error: Error ->
            Stacktrace = erlang:get_stacktrace(),
            try yecc_error_type(Error, Stacktrace) of
                Desc ->
                    erlang:raise(error, {yecc_bug, ?CODE_VERSION, Desc},
                                 Stacktrace)
            catch _:_ -> erlang:raise(error, Error, Stacktrace)
            end;
        %% Probably thrown from return_error/2:
        throw: {error, {_Line, ?MODULE, _M}} = Error ->
            Error
    end.

yecc_error_type(function_clause, [{?MODULE,F,ArityOrArgs,_} | _]) ->
    case atom_to_list(F) of
        "yeccgoto_" ++ SymbolL ->
            {ok,[{atom,_,Symbol}],_} = erl_scan:string(SymbolL),
            State = case ArityOrArgs of
                        [S,_,_,_,_,_,_] -> S;
                        _ -> state_is_unknown
                    end,
            {Symbol, State, missing_in_goto_table}
    end.

yeccpars1([Token | Tokens], Tzr, State, States, Vstack) ->
    yeccpars2(State, element(1, Token), States, Vstack, Token, Tokens, Tzr);
yeccpars1([], {{F, A},_Line}, State, States, Vstack) ->
    case apply(F, A) of
        {ok, Tokens, Endline} ->
            yeccpars1(Tokens, {{F, A}, Endline}, State, States, Vstack);
        {eof, Endline} ->
            yeccpars1([], {no_func, Endline}, State, States, Vstack);
        {error, Descriptor, _Endline} ->
            {error, Descriptor}
    end;
yeccpars1([], {no_func, no_line}, State, States, Vstack) ->
    Line = 999999,
    yeccpars2(State, '$end', States, Vstack, yecc_end(Line), [],
              {no_func, Line});
yeccpars1([], {no_func, Endline}, State, States, Vstack) ->
    yeccpars2(State, '$end', States, Vstack, yecc_end(Endline), [],
              {no_func, Endline}).

%% yeccpars1/7 is called from generated code.
%%
%% When using the {includefile, Includefile} option, make sure that
%% yeccpars1/7 can be found by parsing the file without following
%% include directives. yecc will otherwise assume that an old
%% yeccpre.hrl is included (one which defines yeccpars1/5).
yeccpars1(State1, State, States, Vstack, Token0, [Token | Tokens], Tzr) ->
    yeccpars2(State, element(1, Token), [State1 | States],
              [Token0 | Vstack], Token, Tokens, Tzr);
yeccpars1(State1, State, States, Vstack, Token0, [], {{_F,_A}, _Line}=Tzr) ->
    yeccpars1([], Tzr, State, [State1 | States], [Token0 | Vstack]);
yeccpars1(State1, State, States, Vstack, Token0, [], {no_func, no_line}) ->
    Line = yecctoken_end_location(Token0),
    yeccpars2(State, '$end', [State1 | States], [Token0 | Vstack],
              yecc_end(Line), [], {no_func, Line});
yeccpars1(State1, State, States, Vstack, Token0, [], {no_func, Line}) ->
    yeccpars2(State, '$end', [State1 | States], [Token0 | Vstack],
              yecc_end(Line), [], {no_func, Line}).

%% For internal use only.
yecc_end({Line,_Column}) ->
    {'$end', Line};
yecc_end(Line) ->
    {'$end', Line}.

yecctoken_end_location(Token) ->
    try
        {text, Str} = erl_scan:token_info(Token, text),
        {line, Line} = erl_scan:token_info(Token, line),
        Parts = re:split(Str, "\n"),
        Dline = length(Parts) - 1,
        Yline = Line + Dline,
        case erl_scan:token_info(Token, column) of
            {column, Column} ->
                Col = byte_size(lists:last(Parts)),
                {Yline, Col + if Dline =:= 0 -> Column; true -> 1 end};
            undefined ->
                Yline
        end
    catch _:_ ->
        yecctoken_location(Token)
    end.

-compile({nowarn_unused_function, yeccerror/1}).
yeccerror(Token) ->
    Text = yecctoken_to_string(Token),
    Location = yecctoken_location(Token),
    {error, {Location, ?MODULE, ["syntax error before: ", Text]}}.

-compile({nowarn_unused_function, yecctoken_to_string/1}).
yecctoken_to_string(Token) ->
    case catch erl_scan:token_info(Token, text) of
        {text, Txt} -> Txt;
        _ -> yecctoken2string(Token)
    end.

yecctoken_location(Token) ->
    case catch erl_scan:token_info(Token, location) of
        {location, Loc} -> Loc;
        _ -> element(2, Token)
    end.

-compile({nowarn_unused_function, yecctoken2string/1}).
yecctoken2string({atom, _, A}) -> io_lib:write(A);
yecctoken2string({integer,_,N}) -> io_lib:write(N);
yecctoken2string({float,_,F}) -> io_lib:write(F);
yecctoken2string({char,_,C}) -> io_lib:write_char(C);
yecctoken2string({var,_,V}) -> io_lib:format("~s", [V]);
yecctoken2string({string,_,S}) -> io_lib:write_string(S);
yecctoken2string({reserved_symbol, _, A}) -> io_lib:write(A);
yecctoken2string({_Cat, _, Val}) -> io_lib:format("~p",[Val]);
yecctoken2string({dot, _}) -> "'.'";
yecctoken2string({'$end', _}) ->
    [];
yecctoken2string({Other, _}) when is_atom(Other) ->
    io_lib:write(Other);
yecctoken2string(Other) ->
    io_lib:write(Other).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



-file("src/riak_ql_parser.erl", 276).

yeccpars2(0=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(1=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_1(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(2=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_2(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(3=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_3(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(4=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_4(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(5=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_5(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(6=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_6(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(7=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_7(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(8=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_8(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(9=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_3(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(10=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_10(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(11=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_11(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(12=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_12(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(13=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_13(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(14=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_14(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(15=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_15(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(16=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_16(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(17=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_10(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(18=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_18(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(19=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_19(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(20=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_20(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(21=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_21(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(22=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_22(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(23=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(24=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_24(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(25=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_25(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(26=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_26(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(27=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_27(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(28=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_18(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(29=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_29(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(30=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_30(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(31=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(32=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_32(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(33=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_33(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(34=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_34(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(35=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_35(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(36=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_36(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(37=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(38=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_38(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(39=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(40=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(41=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(42=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_42(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(43=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(44=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(45=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(46=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(47=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_47(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(48=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(49=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_49(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(50=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_50(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(51=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_51(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(52=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_52(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(53=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_53(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(54=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_54(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(55=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_55(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(56=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_56(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(57=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_57(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(58=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_58(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(59=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_59(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(60=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_60(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(61=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_61(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(62=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_62(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(Other, _, _, _, _, _, _) ->
 erlang:error({yecc_bug,"1.4",{missing_state_in_action_table, Other}}).

yeccpars2_0(S, select, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 3, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_1(S, limit, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_1(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Query\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_2(_S, '$end', _Ss, Stack, _T, _Ts, _Tzr) ->
 {ok, hd(Stack)};
yeccpars2_2(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_3(S, chars, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 7, Ss, Stack, T, Ts, Tzr);
yeccpars2_3(S, maybetimes, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 8, Ss, Stack, T, Ts, Tzr);
yeccpars2_3(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_4(S, chars, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_4(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_4_(Stack),
 'yeccgoto_\'Field\''(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_5(S, comma, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 9, Ss, Stack, T, Ts, Tzr);
yeccpars2_5(S, from, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 10, Ss, Stack, T, Ts, Tzr);
yeccpars2_5(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_6(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Fields\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_7(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_7_(Stack),
 'yeccgoto_\'Word\''(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_8(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_8_(Stack),
 'yeccgoto_\'Field\''(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_9: see yeccpars2_3

yeccpars2_10(S, chars, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 7, Ss, Stack, T, Ts, Tzr);
yeccpars2_10(S, quoted, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 14, Ss, Stack, T, Ts, Tzr);
yeccpars2_10(S, regex, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 15, Ss, Stack, T, Ts, Tzr);
yeccpars2_10(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_11(S, chars, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_11(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Bucket\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_12(S, comma, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 17, Ss, Stack, T, Ts, Tzr);
yeccpars2_12(S, where, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 18, Ss, Stack, T, Ts, Tzr);
yeccpars2_12(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_12_(Stack),
 'yeccgoto_\'Select\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_13(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Buckets\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_14(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_14_(Stack),
 'yeccgoto_\'Bucket\''(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_15(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_15_(Stack),
 'yeccgoto_\'Bucket\''(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_16(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_16_(Stack),
 'yeccgoto_\'Select\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_17: see yeccpars2_10

yeccpars2_18(S, chars, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 7, Ss, Stack, T, Ts, Tzr);
yeccpars2_18(S, openb, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 28, Ss, Stack, T, Ts, Tzr);
yeccpars2_18(S, regex, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 29, Ss, Stack, T, Ts, Tzr);
yeccpars2_18(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_19(S, chars, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_19(S, openb, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 55, Ss, Stack, T, Ts, Tzr);
yeccpars2_19(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Vals\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_20(S, approx, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 38, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, div_, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 39, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, eq, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 40, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, gt, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 41, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, lt, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 42, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, maybetimes, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 43, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, minus, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 44, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, ne, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 45, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, nomatch, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 46, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, notapprox, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 47, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, plus, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 48, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_21(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Vals\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_22(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Vals\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_23(S, and_, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 32, Ss, Stack, T, Ts, Tzr);
yeccpars2_23(S, or_, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 34, Ss, Stack, T, Ts, Tzr);
yeccpars2_23(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_23_(Stack),
 'yeccgoto_\'Where\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_24(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Conds\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_25(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Val\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_26(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Val\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_27(S, chars, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 36, Ss, Stack, T, Ts, Tzr);
yeccpars2_27(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Val\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

%% yeccpars2_28: see yeccpars2_18

yeccpars2_29(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Vals\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_30(S, and_, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 32, Ss, Stack, T, Ts, Tzr);
yeccpars2_30(S, closeb, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 33, Ss, Stack, T, Ts, Tzr);
yeccpars2_30(S, or_, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 34, Ss, Stack, T, Ts, Tzr);
yeccpars2_30(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_31(S, chars, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 7, Ss, Stack, T, Ts, Tzr);
yeccpars2_31(S, regex, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 29, Ss, Stack, T, Ts, Tzr);
yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_32(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Logic\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_33(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_33_(Stack),
 'yeccgoto_\'Conds\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_34(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Logic\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_35(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_35_(Stack),
 'yeccgoto_\'Conds\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_36(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_36_(Stack),
 'yeccgoto_\'Val\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_37: see yeccpars2_31

yeccpars2_38(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Comp\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_39(S, datetime, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 25, Ss, Stack, T, Ts, Tzr);
yeccpars2_39(S, float, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 26, Ss, Stack, T, Ts, Tzr);
yeccpars2_39(S, int, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 27, Ss, Stack, T, Ts, Tzr);
yeccpars2_39(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Comp\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Comp\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_42(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Comp\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

%% yeccpars2_43: see yeccpars2_39

%% yeccpars2_44: see yeccpars2_39

yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Comp\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Comp\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_47(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 'yeccgoto_\'Comp\''(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

%% yeccpars2_48: see yeccpars2_39

yeccpars2_49(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_49_(Stack),
 'yeccgoto_\'Vals\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_50(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_50_(Stack),
 'yeccgoto_\'Vals\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_51(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_51_(Stack),
 'yeccgoto_\'Vals\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_52(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_52_(Stack),
 'yeccgoto_\'Vals\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_53(S, div_, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 39, Ss, Stack, T, Ts, Tzr);
yeccpars2_53(S, maybetimes, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 43, Ss, Stack, T, Ts, Tzr);
yeccpars2_53(S, minus, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 44, Ss, Stack, T, Ts, Tzr);
yeccpars2_53(S, plus, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 48, Ss, Stack, T, Ts, Tzr);
yeccpars2_53(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_53_(Stack),
 'yeccgoto_\'Cond\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_54(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_54_(Stack),
 'yeccgoto_\'Word\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_55(S, closeb, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 57, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_56(S, closeb, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 58, Ss, Stack, T, Ts, Tzr);
yeccpars2_56(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_57(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_57_(Stack),
 'yeccgoto_\'Funcall\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_58(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_58_(Stack),
 'yeccgoto_\'Funcall\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_59(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_59_(Stack),
 'yeccgoto_\'Buckets\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_60(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_60_(Stack),
 'yeccgoto_\'Fields\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_61(S, int, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_61(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_62(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_62_(Stack),
 'yeccgoto_\'Query\''(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

'yeccgoto_\'Bucket\''(10=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_13(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Bucket\''(17=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_59(_S, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Buckets\''(10, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_12(12, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Comp\''(20, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(37, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Cond\''(18=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_24(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Cond\''(28=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_24(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Cond\''(31=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_35(_S, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Conds\''(18, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(23, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Conds\''(28, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_30(30, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Field\''(3=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_6(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Field\''(9=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_60(_S, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Fields\''(3, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_5(5, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Funcall\''(18=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_22(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Funcall\''(28=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_22(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Funcall\''(31=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_22(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Funcall\''(37=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_22(_S, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Logic\''(23, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(31, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Logic\''(30, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(31, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Query\''(0, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_2(2, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Select\''(0, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_1(1, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Val\''(18=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_21(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Val\''(28=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_21(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Val\''(31=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_21(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Val\''(37=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_21(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Val\''(39=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_52(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Val\''(43=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_51(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Val\''(44=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_50(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Val\''(48=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_49(_S, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Val\''(55, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_56(56, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Vals\''(18, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_20(20, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Vals\''(28, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_20(20, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Vals\''(31, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_20(20, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Vals\''(37, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_53(53, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Where\''(12=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_16(_S, Cat, Ss, Stack, T, Ts, Tzr).

'yeccgoto_\'Word\''(3, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_4(4, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Word\''(9, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_4(4, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Word\''(10, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_11(11, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Word\''(17, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_11(11, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Word\''(18, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_19(19, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Word\''(28, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_19(19, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Word\''(31, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_19(19, Cat, Ss, Stack, T, Ts, Tzr);
'yeccgoto_\'Word\''(37, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_19(19, Cat, Ss, Stack, T, Ts, Tzr).

-compile({inline,yeccpars2_4_/1}).
-file("src/riak_ql_parser.yrl", 71).
yeccpars2_4_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   log ( __1 , "make field" )
  end | __Stack].

-compile({inline,yeccpars2_7_/1}).
-file("src/riak_ql_parser.yrl", 82).
yeccpars2_7_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   process ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_8_/1}).
-file("src/riak_ql_parser.yrl", 72).
yeccpars2_8_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   log ( __1 , "make field" )
  end | __Stack].

-compile({inline,yeccpars2_12_/1}).
-file("src/riak_ql_parser.yrl", 65).
yeccpars2_12_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_clause ( __1 , __2 , __3 , __4 )
  end | __Stack].

-compile({inline,yeccpars2_14_/1}).
-file("src/riak_ql_parser.yrl", 79).
yeccpars2_14_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   log ( __1 , "buckets -> quoted" )
  end | __Stack].

-compile({inline,yeccpars2_15_/1}).
-file("src/riak_ql_parser.yrl", 78).
yeccpars2_15_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   log ( __1 , "buckets -> regex" )
  end | __Stack].

-compile({inline,yeccpars2_16_/1}).
-file("src/riak_ql_parser.yrl", 64).
yeccpars2_16_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_clause ( __1 , __2 , __3 , __4 , __5 )
  end | __Stack].

-compile({inline,yeccpars2_23_/1}).
-file("src/riak_ql_parser.yrl", 66).
yeccpars2_23_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   make_where ( __1 , __2 )
  end | __Stack].

-compile({inline,yeccpars2_33_/1}).
-file("src/riak_ql_parser.yrl", 87).
yeccpars2_33_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_expr ( __2 )
  end | __Stack].

-compile({inline,yeccpars2_35_/1}).
-file("src/riak_ql_parser.yrl", 88).
yeccpars2_35_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_expr ( __1 , __2 , __3 )
  end | __Stack].

-compile({inline,yeccpars2_36_/1}).
-file("src/riak_ql_parser.yrl", 102).
yeccpars2_36_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   add_unit ( __1 , __2 )
  end | __Stack].

-compile({inline,yeccpars2_49_/1}).
-file("src/riak_ql_parser.yrl", 93).
yeccpars2_49_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_expr ( __1 , __2 , __3 )
  end | __Stack].

-compile({inline,yeccpars2_50_/1}).
-file("src/riak_ql_parser.yrl", 94).
yeccpars2_50_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_expr ( __1 , __2 , __3 )
  end | __Stack].

-compile({inline,yeccpars2_51_/1}).
-file("src/riak_ql_parser.yrl", 95).
yeccpars2_51_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_expr ( __1 , __2 , __3 )
  end | __Stack].

-compile({inline,yeccpars2_52_/1}).
-file("src/riak_ql_parser.yrl", 96).
yeccpars2_52_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_expr ( __1 , __2 , __3 )
  end | __Stack].

-compile({inline,yeccpars2_53_/1}).
-file("src/riak_ql_parser.yrl", 91).
yeccpars2_53_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_expr ( __1 , __2 , __3 )
  end | __Stack].

-compile({inline,yeccpars2_54_/1}).
-file("src/riak_ql_parser.yrl", 81).
yeccpars2_54_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   concatenate ( __1 , __2 )
  end | __Stack].

-compile({inline,yeccpars2_57_/1}).
-file("src/riak_ql_parser.yrl", 84).
yeccpars2_57_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_funcall ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_58_/1}).
-file("src/riak_ql_parser.yrl", 85).
yeccpars2_58_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_funcall ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_59_/1}).
-file("src/riak_ql_parser.yrl", 74).
yeccpars2_59_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_list ( __1 , __3 )
  end | __Stack].

-compile({inline,yeccpars2_60_/1}).
-file("src/riak_ql_parser.yrl", 68).
yeccpars2_60_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   make_list ( __1 , __3 )
  end | __Stack].

-compile({inline,yeccpars2_62_/1}).
-file("src/riak_ql_parser.yrl", 61).
yeccpars2_62_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   add_limit ( __1 , __2 , __3 )
  end | __Stack].


-file("src/riak_ql_parser.yrl", 211).
