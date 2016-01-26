%% -------------------------------------------------------------------
%%
%% riak_ql_ddl_compiler: processes the Data Description in the DDL
%% * verifies that the DDL is valid
%% * compiles the record description in the DDL into a module that can
%%   be used to verify that incoming data conforms to a schema at the boundary
%%
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% TODO
%% * are we usings lists to describe fields or what?
%% * turning bucket names into verification modules is a potential
%%   memory leak on the atom table - not much we can do about that...
%% * do we care about the name of the validation module?

%%
%% Getting started with ASTs
%%
%% the easiest way to understand the Erlang AST is to compile
%% some simple modules to AST and look at the format
%%
%% This is how you do that:
%% * write a module - mymodule.erl
%% * fire up a shell in the same directory
%% * call the erlang pre-processor on it
%%   >epp:parse_file("mymodule.erl", [], []).
%%
%% if you have include files in your module you wil need to
%% set the include path etc, etc

%% @doc
-module(riak_ql_ddl_compiler).

-include("riak_ql_ddl.hrl").

-export([compile/1]).
-export([compile_and_load_from_tmp/1]).
-export([write_source_to_files/3]).

-define(IGNORE,        true).
-define(DONTIGNORE,    false).
-define(NOPREFIX,      []).

%% you have to start line and positions nos somewhere
-define(POSNOSTART,  1).
-define(LINENOSTART, 1).
-define(ARITYOF1,    1).

-type expr()   :: tuple().
-type exprs()  :: [expr()].
-type guards() :: [exprs()].
-type ast()    :: [expr() | exprs() | guards()].
%% maps() are an aggregration of [map()]
-type maps()   :: {maps, [[#riak_field_v1{}]]}.
-type map()    :: {map,  [#riak_field_v1{}]}.

%% Compile the DDL to its helper module AST.
-spec compile(#ddl_v1{}) ->
        {module, ast()} | {error, tuple()}.
compile({ok, #ddl_v1{} = DDL}) ->
    % handle output directly from riak_ql_parser
    compile(DDL);
compile(#ddl_v1{ table = Table, fields = Fields } = DDL) ->
    {ModName, Attrs, LineNo} = make_attrs(Table, ?LINENOSTART),
    {VFns,  LineNo2} = build_validn_fns([Fields],   LineNo,  ?POSNOSTART, [], []),
    {ACFns, LineNo3} = build_add_cols_fns(Fields, LineNo2),
    {ExtractFn, LineNo4} = build_extract_fn([Fields],  LineNo3, []),
    {GetTypeFn, LineNo5} = build_get_type_fn([Fields], LineNo4, []),
    {IsValidFn, LineNo6} = build_is_valid_fn([Fields], LineNo5, []),
    {GetDDLFn, LineNo7}  = build_get_ddl_fn(DDL, LineNo6, []),
    AST = Attrs
        ++ VFns
        ++ ACFns
        ++ [ExtractFn, GetTypeFn, IsValidFn, GetDDLFn, {eof, LineNo7}],
    case erl_lint:module(AST) of
        {ok, []} ->
            {ModName, AST};
        Other ->
            exit(Other)
    end.

%% Compile the module, write it to /tmp then load it into the VM.
-spec compile_and_load_from_tmp(#ddl_v1{}) ->
        {module, atom()} | {error, tuple()}.
compile_and_load_from_tmp({ok, #ddl_v1{} = DDL}) ->
    % handle output directly from riak_ql_parser
    compile_and_load_from_tmp(DDL);
compile_and_load_from_tmp(#ddl_v1{} = DDL) ->
    {Module, AST} = compile(DDL),
    {ok, Module, Bin} = compile:forms(AST),
    BeamFileName = "/tmp/" ++ atom_to_list(Module) ++ ".beam",
    {module, Module} = code:load_binary(Module, BeamFileName, Bin).

%% Write the AST and erlang source derived from the AST to disk for debugging.
-spec write_source_to_files(string(), #ddl_v1{}, ast()) -> ok.
write_source_to_files(OutputDir, #ddl_v1{ table = Table } = DDL, AST) ->
    ModName = riak_ql_ddl:make_module_name(Table),
    ASTFileName = filename:join([OutputDir, ModName]) ++ ".ast",
    SrcFileName = filename:join([OutputDir, ModName]) ++ ".erl",
    ok = write_file(ASTFileName, io_lib:format("~p", [AST])),
    ok = write_file(SrcFileName, [
        build_debug_header_text(DDL), "\n",
        build_erlang_source(AST)]).

%%
build_erlang_source(AST1) ->
    AST2 = filter_ast(AST1, []),
    Syntax = erl_syntax:form_list(AST2),
    erl_prettypr:format(Syntax).

%%
build_debug_header_text(#ddl_v1{ table = Table, fields = Fields,
                                 partition_key = PartitionKey,
                                 local_key = LocalKey }) ->
    io_lib:format(
        "%%% Generated Module, DO NOT EDIT~n~n"
        "%%% Validates the DDL~n~n"
        "%%% Table         : ~s~n"
        "%%% Fields        : ~p~n"
        "%%% Partition_Key : ~p~n"
        "%%% Local_Key     : ~p~n~n",
        [Table, Fields, PartitionKey, LocalKey ]).

%%
write_file(FileName, Contents) ->
    ok = filelib:ensure_dir(FileName),
    ok = file:write_file(FileName, [Contents]).

filter_ast([], Acc) ->
    lists:reverse(Acc);
filter_ast([{eof, _} | T], Acc) ->
    filter_ast(T, Acc);
filter_ast([H | T], Acc) ->
    case element(3, H) of
        file      -> filter_ast(T, Acc);
        _         -> filter_ast(T, [H | Acc])
    end.

-spec build_extract_fn([[#riak_field_v1{}]], pos_integer(), ast()) ->
                              {expr(), pos_integer()}.
build_extract_fn([], LineNo, Acc) ->
    Clauses = lists:flatten(lists:reverse(Acc)),
    Fn = make_fun(extract, 2, Clauses, LineNo),
    {Fn, LineNo};
build_extract_fn([Fields | T], LineNo, Acc) ->
    {Fns, LineNo2} = make_extract_cls(Fields, LineNo, ?NOPREFIX, []),
    build_extract_fn(T, LineNo2, [Fns | Acc]).

make_extract_cls([], LineNo, _Prefix, Acc) ->
    {lists:reverse(Acc), LineNo + 1};
make_extract_cls([#riak_field_v1{type = Ty} = H | T], LineNo, Prefix, Acc) ->
    NPref  = [H | Prefix],
    Args   = [make_string(binary_to_list(Nm), LineNo)
	      || #riak_field_v1{name = Nm} <- NPref],
    Poses  = [make_integer(P,  LineNo) || #riak_field_v1{position = P}  <- NPref],
    Conses = make_conses(Args, LineNo, {nil, LineNo}),
    Var    = make_var('Obj', LineNo),
    %% you need to reverse the lists of the positions to
    %% get the calls to element to nest correctly
    Body = make_elem_calls(lists:reverse(Poses), LineNo, Var),
    Guard = make_call(make_atom(is_tuple, LineNo), [Var], LineNo),
    Cl = make_clause([Var, Conses], [[Guard]], Body, LineNo),
    {NewA, NewLineNo} =
        case Ty of
            {map, MapFields} ->
                {Cls, NLNo} = make_extract_cls(MapFields, LineNo, NPref, Acc),
                {[Cl | Cls], NLNo};
            _ ->
                {[Cl | Acc], LineNo}
        end,
    make_extract_cls(T, NewLineNo, Prefix, NewA).

%% this is gnarly because the field order is compile-time dependent
-spec build_get_ddl_fn(#ddl_v1{}, pos_integer(), ast()) ->
			      {expr(), pos_integer()}.
build_get_ddl_fn(#ddl_v1{table         = T,
			 fields        = F,
			 partition_key = PK,
			 local_key     = LK}, LineNo, []) ->
    PosT  = #ddl_v1.table,
    PosF  = #ddl_v1.fields,
    PosPK = #ddl_v1.partition_key,
    PosLK = #ddl_v1.local_key,
    %% the order is dependent on the order the fields are listed in the record at
    %% compile time hence this rather obscure dance of the positions
    %% The record name is deliberatly put last in the list to check it is
    %% actually working ;-)
    {_Poses, List} = lists:unzip(lists:sort([
					     {PosT,  make_binary(T, LineNo)},
					     {PosF,  expand_fields(F, LineNo)},
					     {PosPK, expand_key(PK, LineNo)},
					     {PosLK, expand_key(LK, LineNo)},
					     {1,     make_atom(ddl_v1, LineNo)}
					    ])),
    Body = make_tuple(List, LineNo),
    Cl = make_clause([], [], Body, LineNo),
    Fn = make_fun(get_ddl, 0, [Cl], LineNo),
    {Fn, LineNo + 1}.

expand_fields(Fs, LineNo) ->
    Fields = [expand_field(X, LineNo) || X <- Fs],
    make_conses(lists:reverse(Fields), LineNo, {nil, LineNo}).

expand_field(#riak_field_v1{name     = Nm,
			    position = Pos,
			    type     = Ty,
			    optional = Opt}, LineNo) ->
    PosNm  =  #riak_field_v1.name,
    PosPos =  #riak_field_v1.position,
    PosTy  =  #riak_field_v1.type,
    PosOpt = #riak_field_v1.optional,
    {_Poses, List} = lists:unzip(lists:sort([
					     {PosNm,  make_binary(Nm, LineNo)},
					     {PosPos, make_integer(Pos, LineNo)},
					     {PosTy,  expand_type(Ty, LineNo)},
					     {PosOpt, make_atom(Opt, LineNo)},
					     {1,      make_atom(riak_field_v1, LineNo)}
					    ])),
    make_tuple(List, LineNo).

expand_key(none, LineNo) ->
    make_atom(none, LineNo);
expand_key(#key_v1{ast = []}, LineNo) ->
    make_tuple([make_atom(key_v1, LineNo) ,{nil, LineNo}], LineNo);
expand_key(#key_v1{ast = AST}, LineNo) ->
    Rest = expand_ast(AST, LineNo),
    make_tuple([make_atom(key_v1, LineNo) | [Rest]], LineNo).

expand_ast(AST, LineNo) when is_list(AST) ->
    Fields = [expand_a2(X, LineNo) || X <- AST],
    make_conses(lists:reverse(Fields), LineNo, {nil, LineNo}).

expand_a2(#param_v1{name = Nm}, LineNo) ->
    Bins = [make_binary(X, LineNo) || X <- Nm],
    Conses = make_conses(Bins, LineNo, {nil, LineNo}),
    make_tuple([make_atom(param_v1, LineNo) | [Conses]], LineNo);
expand_a2(#hash_fn_v1{mod  = Mod,
		      fn   = Fn,
		      args = Args,
		      type = Ty}, LineNo) ->
    PosMod  = #hash_fn_v1.mod,
    PosFn   = #hash_fn_v1.fn,
    PosArgs = #hash_fn_v1.args,
    PosType = #hash_fn_v1.type,
    {_Pos, List} = lists:unzip(lists:sort([
					   {PosMod,  make_atom(Mod, LineNo)},
					   {PosFn,   make_atom(Fn, LineNo)},
					   {PosArgs, expand_args(Args, LineNo)},
					   {PosType, make_atom(Ty, LineNo)},
					   {1,       make_atom(hash_fn_v1, LineNo)}
					  ])),
    make_tuple(List, LineNo).

expand_args(Args, LineNo) ->
    Args2 = lists:reverse([expand_args2(X, LineNo) || X <- Args]),
    make_conses(Args2, LineNo, {nil, LineNo}).

%% this first clause jumps out to a different expansion tree
%% to expand the parameter in the fuction args
expand_args2(Arg, LineNo) when is_record(Arg, param_v1) ->
    expand_a2(Arg, LineNo);
expand_args2(Arg, LineNo) when is_binary(Arg) ->
    make_binary(Arg, LineNo);
expand_args2(Arg, LineNo) when is_integer(Arg) ->
    make_integer(Arg, LineNo);
expand_args2(Arg, LineNo) when is_float(Arg) ->
    make_float(Arg, LineNo);
expand_args2(Arg, LineNo) when is_list(Arg) ->
    make_string(Arg, LineNo);
expand_args2(Arg, LineNo) when is_atom(Arg) ->
    make_atom(Arg, LineNo).

expand_type({map, Fs}, LineNo) ->
    Fields = lists:reverse([expand_field(X, LineNo) || X <- Fs]),
    Conses = make_conses(Fields, LineNo, {nil, LineNo}),
    make_tuple([make_atom(map, LineNo), Conses], LineNo);
expand_type(Type, LineNo) ->
    make_atom(Type, LineNo).

-spec build_is_valid_fn([[#riak_field_v1{}]], pos_integer(), ast()) ->
			       {expr(), pos_integer()}.
build_is_valid_fn([], LineNo, Acc) ->
    {Fail, NewLineNo} = make_fail_clause(LineNo),
    Clauses = lists:flatten(lists:reverse([Fail | Acc])),
    Fn = make_fun(is_field_valid, 1, Clauses, LineNo),
    {Fn, NewLineNo};
build_is_valid_fn([Fields | T], LineNo, Acc) ->
    {Fns, LineNo2} = make_is_valid_cls(Fields, LineNo, ?NOPREFIX, []),
    build_is_valid_fn(T, LineNo2, [Fns | Acc]).

make_is_valid_cls([], LineNo, Prefix, Acc) ->
    %% handle the prefixes slightly differently here than to the next clause
    Args = [make_string(binary_to_list(Nm), LineNo)
	    || #riak_field_v1{name = Nm} <- Prefix],
    WildArgs = [make_string("*", LineNo) | Args],
    WildConses = make_conses(WildArgs, LineNo, {nil, LineNo}),
    Body = make_atom(true, LineNo),
    Guard = [],
    WildCl = make_clause([WildConses], Guard, Body, LineNo),
    {lists:reverse([WildCl | Acc]), LineNo};
make_is_valid_cls([#riak_field_v1{type = Ty} = H | T], LineNo, Prefix, Acc) ->
    %% handle the prefixes slightly differently here than to the perviousls clause
    NPref  = [H | Prefix],
    Args   = [make_string(binary_to_list(Nm), LineNo)
	      || #riak_field_v1{name = Nm} <- NPref],
    Conses = make_conses(Args, LineNo, {nil, LineNo}),
    %% you need to reverse the lists of the positions to
    %% get the calls to element to nest correctly
    Body = make_atom(true, LineNo),
    Guard = [],
    Cl = make_clause([Conses], Guard, Body, LineNo),
    {NewA, NewLineNo} =
        case Ty of
            {map, MapFields} ->
                {Cls, NLNo} = make_is_valid_cls(MapFields, LineNo, NPref, Acc),
                {[Cl | Cls], NLNo};
            _ ->
                {[Cl | Acc], LineNo}
        end,
    make_is_valid_cls(T, NewLineNo, Prefix, NewA).

-spec build_get_type_fn([[#riak_field_v1{}]], pos_integer(), ast()) ->
			       {expr(), pos_integer()}.
build_get_type_fn([], LineNo, Acc) ->
    Clauses = lists:flatten(lists:reverse(Acc)),
    Fn = make_fun(get_field_type, 1, Clauses, LineNo),
    {Fn, LineNo + 1};
build_get_type_fn([Fields | T], LineNo, Acc) ->
    {Fns, LineNo2} = make_get_type_cls(Fields, LineNo, ?NOPREFIX, []),
    build_get_type_fn(T, LineNo2, [Fns | Acc]).

make_get_type_cls([], LineNo, _Prefix, Acc) ->
    {lists:reverse(Acc), LineNo};
make_get_type_cls([#riak_field_v1{type = Ty} = H | T], LineNo, Prefix, Acc) ->
    NPref  = [H | Prefix],
    Args   = [make_string(binary_to_list(Nm), LineNo)
	      || #riak_field_v1{name = Nm} <- NPref],
    Conses = make_conses(Args, LineNo, {nil, LineNo}),
    %% you need to reverse the lists of the positions to
    %% get the calls to element to nest correctly
    Body = case Ty of
	       {map, _}  -> make_atom(map, LineNo);
	       _         -> make_atom(Ty, LineNo)
	   end,
    Guard = [],
    Cl = make_clause([Conses], Guard, Body, LineNo),
    {NewA, NewLineNo} =
        case Ty of
            {map, MapFields} ->
                {Cls, NLNo} = make_get_type_cls(MapFields, LineNo, NPref, Acc),
                {[Cl | Cls], NLNo};
            _ ->
                {[Cl | Acc], LineNo}
        end,
    make_get_type_cls(T, NewLineNo, Prefix, NewA).

make_conses([], _LineNo, Conses)  -> Conses;
make_conses([H | T], LineNo, Acc) -> NewAcc = {cons, LineNo, H, Acc},
                                     make_conses(T, LineNo, NewAcc).

make_elem_calls([], _LineNo, ElemCs)  -> ElemCs;
make_elem_calls([H | T], LineNo, Acc) -> E = make_atom(element, LineNo),
                                         NewA = make_call(E, [H, Acc], LineNo),
                                         make_elem_calls(T, LineNo, NewA).

build_add_cols_fns(Fields, LineNo) ->
    {Args, Success} = make_args_and_fields(Fields, LineNo),
    {ClauseS, LineNo2} = make_success_clause(Args, [], Success, LineNo),
    Fun = make_fun(add_column_info, 1, [ClauseS], LineNo2),
    {[Fun], LineNo2}.

make_args_and_fields(Fields, LineNo) ->
    make_ar2(Fields, LineNo, [], []).

make_ar2([], LineNo, Acc1, Acc2) ->
    {make_tuple(lists:flatten(lists:reverse(Acc1)), LineNo),
     make_conses(lists:flatten(Acc2), LineNo, {nil, LineNo})};
make_ar2([#riak_field_v1{name = Nm,
			 type = {map, M}} | T], LineNo, Acc1, Acc2) ->
    {NewArg1, NewF1} = make_args_and_fields(M, LineNo),
    NewNm = make_string(binary_to_list(Nm), LineNo),
    NewF2 = make_tuple([NewNm, NewF1], LineNo),
    make_ar2(T, LineNo, [NewArg1 | Acc1], [NewF2 | Acc2]);
make_ar2([#riak_field_v1{name = Nm} = F | T], LineNo, Acc1, Acc2) ->
    [NewAcc1] = make_names([F], LineNo),
    NewAcc2 = make_tuple([make_string(binary_to_list(Nm), LineNo), NewAcc1], LineNo),
    make_ar2(T, LineNo, [NewAcc1 | Acc1], [NewAcc2 | Acc2]).

%%% this is a messy function because the first validation clause has
%%% a simple tuple as an arguement, but the subsequent ones (that validate
%% the data in embedded maps) take a tuple of tuples
-spec build_validn_fns([[#riak_field_v1{}] | [maps()]],
                       pos_integer(), pos_integer(),
                       [maps()], [ast()]) ->
                              {[ast()], pos_integer()}.
build_validn_fns([], LineNo, _, [], Acc2) ->
    {lists:flatten(lists:reverse(Acc2)), LineNo + 1};
build_validn_fns([], LineNo, FunNo, Acc1, Acc2) ->
    build_validn_fns(Acc1, LineNo, FunNo, [], Acc2);
build_validn_fns([Fields | T], LineNo, FunNo, Acc1, Acc2) ->
    {Funs, LineNo2, Maps, FunNo2} = make_validation_funs(Fields, LineNo, FunNo),
    Maps2 = {maps, [Y || {_X, Y} <- Maps]},
    Acc1a = case Maps of
                [] -> Acc1;
                _  -> [Maps2 |  Acc1]
            end,
    build_validn_fns(T, LineNo2, FunNo2, Acc1a, [Funs | Acc2]).

-spec make_attrs(binary(), pos_integer()) -> {atom(), ast(), pos_integer()}.
make_attrs(Bucket, LineNo) when is_binary(Bucket)    ->
    ModName = riak_ql_ddl:make_module_name(Bucket),
    {ModAttr, LineNo1} = make_module_attr(ModName, LineNo),
    {ExpAttr, LineNo2} = make_export_attr(LineNo1),
    {ModName, [ModAttr, ExpAttr], LineNo2}.

-spec make_validation_funs([#riak_field_v1{} | maps()],
                           pos_integer(), pos_integer()) ->
                                  {ast(), pos_integer(),
                                   [map()], pos_integer()}.
make_validation_funs(Fields, LineNo, FunNo) ->
    {Args, Fields2} = case Fields of
                          {maps, MapFields} ->
                              Fs  = [make_names(X, LineNo) || X <- MapFields],
                              Fs2 = [make_tuple(X, LineNo) || X <- Fs],
                              FlatFields = lists:flatten(MapFields),
                              {make_tuple(Fs2, LineNo), FlatFields};
                          _ ->
                              Fs = make_names(Fields, LineNo),
                              {make_tuple(Fs, LineNo), Fields}
                      end,
    {Guards, Maps, LineNo2} = make_validation_guards(Fields2, LineNo + 1),
    Success = case Maps of
                  [] -> make_atom(true, LineNo2);
                  _  -> make_map_call(Maps, LineNo2, FunNo + 1)
              end,
    {ClauseS, _LineNo3} = make_success_clause(Args, Guards, Success, LineNo2),
    {ClauseF, _LineNo4} = make_fail_clause(LineNo2),
    FunName = get_fn_name(validate_obj, FunNo),
    Fun = make_fun(FunName, 1, [ClauseS, ClauseF], LineNo2),
    {[Fun], LineNo2, Maps, FunNo + 1}.

-spec make_fun(atom(), 0 | pos_integer(), ast(), pos_integer()) -> expr().
make_fun(FunName, Arity, Clause, LineNo) ->
    {function, LineNo, FunName, Arity, Clause}.

-spec make_map_call([{expr(), [#riak_field_v1{}]}], pos_integer(), pos_integer()) ->
                           expr().
make_map_call(Fields, LineNo, FunNo) ->
    Arg = make_tuple([make_var(X, LineNo) || {{_, _, X}, _} <- Fields], LineNo),
    Name = get_fn_name(validate_obj, FunNo),
    Fn = make_atom(Name, LineNo),
    _Expr = make_call(Fn, [Arg], LineNo).

-spec make_validation_guards([#riak_field_v1{}], pos_integer()) ->
                                    {ast(), [{expr(), [#riak_field_v1{}]}], pos_integer()}.
make_validation_guards(Fields, LineNo) ->
    {_G1, _Maps, _LineNo2} = make_v_gds2(Fields, LineNo, [], []).

make_v_gds2([], LineNo, [], Acc2) ->
    {[], Acc2, LineNo};
make_v_gds2([], LineNo, Acc1, Acc2) ->
    {[Acc1], Acc2, LineNo};
make_v_gds2([#riak_field_v1{type = any} | T], LineNo, Acc1, Acc2) ->
    make_v_gds2(T, LineNo, Acc1, Acc2);
make_v_gds2([#riak_field_v1{name     = Name,
                            position = NPos,
                            type     = {map, MapFields},
                            optional = false} | T], LineNo, Acc1, Acc2)
  when is_list(MapFields) ->
    Nm = make_name(Name, ?DONTIGNORE, LineNo, NPos),
    make_v_gds2(T, LineNo, Acc1, [{Nm, MapFields} | Acc2]);
make_v_gds2([#riak_field_v1{name     = Name,
                            position = NPos,
                            type     = Type,
                            optional = IsOp} | T], LineNo, Acc1, Acc2)
  when Type =:= varchar   orelse
       Type =:= sint64    orelse
       Type =:= double    orelse
       Type =:= boolean   orelse
       Type =:= set       orelse
       Type =:= timestamp ->
    Nm = make_name(Name, ?DONTIGNORE, LineNo, NPos),
    Call = case IsOp of
               false -> make_validation_guard(Nm, Type, LineNo);
               true  -> LHS = make_op('=:=', Nm, {nil, LineNo}, LineNo),
                        RHS = make_validation_guard(Nm, Type, LineNo),
                        make_op('orelse', LHS, RHS, LineNo)
           end,
    Acc1a = case Acc1 of
                [] -> [Call];
                _  -> [Call | Acc1]
            end,
    make_v_gds2(T, LineNo, Acc1a, Acc2).

-spec make_op(atom(), expr(), expr(), pos_integer()) -> expr().
make_op(Op, LHS, RHS, LineNo) -> {op, LineNo, Op, LHS, RHS}.

-spec make_validation_guard(expr(), simple_field_type(), pos_integer()) -> expr().
make_validation_guard(Nm, varchar, LNo) ->
    {call, LNo, make_atom(is_binary,  LNo), [Nm]};
make_validation_guard(Nm, sint64, LNo) ->
    {call, LNo, make_atom(is_integer, LNo), [Nm]};
make_validation_guard(Nm, double, LNo) ->
    {call, LNo, make_atom(is_float, LNo), [Nm]};
make_validation_guard(Nm, boolean, LNo) ->
    {call, LNo, make_atom(is_boolean, LNo), [Nm]};
make_validation_guard(Nm, set, LNo) ->
    {call, LNo, make_atom(is_list, LNo), [Nm]};
make_validation_guard(Nm, timestamp, LNo) ->
    LHS = {call, LNo, make_atom(is_integer, LNo), [Nm]},
    RHS = make_op('>', Nm, make_integer(0, LNo), LNo),
    make_op('andalso', LHS, RHS, LNo).

%% -spec make_guard(expr(), expr(), atom(), pos_integer) -> expr().
%% make_guard(Nm, RecName, is_record, LineNo) ->
%%     Var = make_var(Nm, LineNo),
%%     {call, LineNo, make_atom(is_record, LineNo), [Var, RecName]}.

-spec make_binary(binary(), pos_integer()) -> expr().
make_binary(B, LineNo) when is_binary(B) ->
    {bin, LineNo, [{bin_element, LineNo, {string, LineNo, binary_to_list(B)}, default, default}]}.

-spec make_float(float(), pos_integer()) -> expr().
make_float(F, LineNo) when is_float(F) -> {float, LineNo, F}.

-spec make_integer(integer(), pos_integer()) -> expr().
make_integer(I, LineNo) when is_integer(I) -> {integer, LineNo, I}.

-spec make_atom(atom(), pos_integer()) -> expr().
make_atom(A, LineNo) when is_atom(A) -> {atom, LineNo, A}.

-spec make_call(expr(), ast(), pos_integer()) -> expr().
make_call(FnName, Args, LineNo) ->
    {call, LineNo, FnName, Args}.

-spec make_names([#riak_field_v1{}], pos_integer()) -> [expr()].
make_names(Fields, LineNo) ->
    Make_fn = fun(#riak_field_v1{name     = Name,
                                 position = NPos,
                                 type     = Type}) ->
                      Make = case Type of
                                 any -> ?IGNORE;
                                 _   -> ?DONTIGNORE
                             end,
                      make_name(Name, Make, LineNo, NPos)
              end,
    [Make_fn(X) || X <- Fields].

-spec make_name(binary(), boolean(), pos_integer(), pos_integer()) -> expr().
make_name(Name, HasPrefix, LineNo, NPos) ->
    Prefix = if
                 HasPrefix -> "_";
                 el/=se    -> ""
             end,
    Nm = binary_to_atom(iolist_to_binary([Prefix, "Var", integer_to_list(NPos), "_", Name]), utf8),
    make_var(Nm, LineNo).

-spec make_var(atom(), pos_integer()) -> expr().
make_var(Name, LineNo) -> {var, LineNo, Name}.

-spec make_success_clause(tuple(), guards(), expr(), pos_integer()) -> {expr(), pos_integer()}.
make_success_clause(Tuple, Guards, Body, LineNo) ->
    Clause = {clause, LineNo, [Tuple], Guards, [Body]},
    {Clause, LineNo + 1}.

-spec make_fail_clause(pos_integer()) -> expr().
make_fail_clause(LineNo) ->
    Var = make_var('_', LineNo),
    False = make_atom(false, LineNo),
    Clause = make_clause([Var], [], False, LineNo),
    {Clause, LineNo + 1}.

-spec get_fn_name(atom(), pos_integer()) -> atom().
get_fn_name(Root, 1) when is_atom(Root) ->
    Root;
get_fn_name(Root, FunNo) when is_atom(Root) andalso
			      is_integer(FunNo) ->
    list_to_atom(atom_to_list(Root) ++ integer_to_list(FunNo)).

-spec make_clause(ast(), guards(), expr(), pos_integer()) -> expr().
make_clause(Args, Guards, Body, LineNo) ->
    {clause, LineNo, Args, Guards, [Body]}.

make_string(String, LineNo) when is_list(String) ->
    {bin,LineNo,[bin_element_string(String, LineNo)]}.

bin_element_string(String, LineNo) ->
    {bin_element,LineNo,{string,LineNo,String},default,default}.

make_tuple(Fields, LineNo) ->
    {tuple, LineNo, Fields}.

make_module_attr(ModName, LineNo) ->
    {{attribute, LineNo, module, ModName}, LineNo + 1}.

make_export_attr(LineNo) ->
    {{attribute, LineNo, export, [
                                  {validate_obj,    1},
				  {add_column_info, 1},
                                  {get_field_type,  1},
                                  {is_field_valid,  1},
                                  {extract,         2},
                                  {get_ddl,         0}
                                 ]}, LineNo + 1}.

-ifdef(TEST).
-compile(export_all).

-define(VALID,   true).
-define(INVALID, false).

-include_lib("eunit/include/eunit.hrl").

%%
%% Helper fns
%%

make_ddl(#ddl_v1{table = Table,
                 fields = Fields,
                 partition_key = PK,
                 local_key = LK
                }) ->
    make_ddl(Table, Fields, PK, LK).

make_ddl(Table, Fields) when is_binary(Table) ->
    make_ddl(Table, Fields, #key_v1{}, #key_v1{}).

make_ddl(Table, Fields, PK) when is_binary(Table) ->
    make_ddl(Table, Fields, PK, #key_v1{}).

make_ddl(Table, Fields, #key_v1{} = PK, #key_v1{} = LK)
  when is_binary(Table) ->
    #ddl_v1{table         = Table,
            fields        = Fields,
            partition_key = PK,
            local_key     = LK}.

%%
%% Unit Tests
%%

simplest_valid_test() ->
    DDL = make_simplest_valid_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>}),
    ?assertEqual(?VALID, Result).

make_simplest_valid_ddl() ->
    make_ddl(<<"simplest_valid_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = varchar}
	     ]).

simple_valid_varchar_test() ->
    DDL = make_simple_valid_varchar_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, <<"werewr">>}),
    ?assertEqual(?VALID, Result).

make_simple_valid_varchar_ddl() ->
    make_ddl(<<"simple_valid_varchar_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = varchar},
	      #riak_field_v1{name     = <<"erko">>,
			     position = 2,
			     type     = varchar}
	     ]).

simple_valid_sint64_test() ->
    DDL = make_simple_valid_sint64_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({999, -9999, 0}),
    ?assertEqual(?VALID, Result).

make_simple_valid_sint64_ddl() ->
    make_ddl(<<"simple_valid_sint64_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = sint64},
	      #riak_field_v1{name     = <<"erko">>,
			     position = 2,
			     type     = sint64},
	      #riak_field_v1{name     = <<"erkle">>,
			     position = 3,
			     type     = sint64}
	     ]).

simple_valid_double_test() ->
    DDL = make_simple_valid_double_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({432.22, -23423.22, -0.0}),
    ?assertEqual(?VALID, Result).

make_simple_valid_double_ddl() ->
    make_ddl(<<"simple_valid_double_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = double},
	      #riak_field_v1{name     = <<"erko">>,
			     position = 2,
			     type     = double},
	      #riak_field_v1{name     = <<"erkle">>,
			     position = 3,
			     type     = double}
	     ]).

simple_valid_boolean_test() ->
    DDL = make_simple_valid_boolean_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({true, false}),
    ?assertEqual(?VALID, Result).

make_simple_valid_boolean_ddl() ->
    make_ddl(<<"simple_valid_boolean_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = boolean},
	      #riak_field_v1{name     = <<"erko">>,
			     position = 2,
			     type     = boolean}
	     ]).

simple_valid_timestamp_test() ->
    DDL = make_simple_valid_timestamp_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({234324, 23424, 34636}),
    ?assertEqual(?VALID, Result).

make_simple_valid_timestamp_ddl() ->
    make_ddl(<<"simple_valid_timestamp_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = timestamp},
	      #riak_field_v1{name     = <<"erko">>,
			     position = 2,
			     type     = timestamp},
	      #riak_field_v1{name     = <<"erkle">>,
			     position = 3,
			     type     = timestamp}
	     ]).

simple_valid_map_1_test() ->
    DDL = make_simple_valid_map_1_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, {<<"erko">>}, 4.4}),
    ?assertEqual(?VALID, Result).

make_simple_valid_map_1_ddl() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yarple">>,
                                position = 1,
                                type     = varchar}
                ]},
    _DDL = make_ddl(<<"simple_valid_map_1_test">>,
		    [
		     #riak_field_v1{name     = <<"yando">>,
				    position = 1,
				    type     = varchar},
		     #riak_field_v1{name     = <<"erko">>,
				    position = 2,
				    type     = Map},
		     #riak_field_v1{name     = <<"erkle">>,
				    position = 3,
				    type     = double}
		    ]).

simple_valid_map_2_test() ->
    DDL = make_simple_valid_map_2_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, {<<"erko">>, -999}, 4.4}),
    ?assertEqual(?VALID, Result).

make_simple_valid_map_2_ddl() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yarple">>,
                                position = 1,
                                type     = varchar},
                 #riak_field_v1{name     = <<"yoplait">>,
                                position = 2,
                                type     = sint64}
                ]},
    _DDL = make_ddl(<<"simple_valid_map_2_test">>,
		    [
		     #riak_field_v1{name     = <<"yando">>,
				    position = 1,
				    type     = varchar},
		     #riak_field_v1{name     = <<"erko">>,
				    position = 2,
				    type     = Map},
		     #riak_field_v1{name     = <<"erkle">>,
				    position = 3,
				    type     = double}
		    ]).

simple_valid_optional_1_test() ->
    DDL = make_simple_valid_optional_1_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({[]}),
    ?assertEqual(?VALID, Result).

make_simple_valid_optional_1_ddl() ->
    make_ddl(<<"simple_valid_optional_1_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = varchar,
			     optional = true}
	     ]).

simple_valid_optional_2_test() ->
    DDL = make_simple_valid_optional_2_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({[], [], [], [], [], [], []}),
    ?assertEqual(?VALID, Result).

make_simple_valid_optional_2_ddl() ->
    make_ddl(<<"simple_valid_optional_2_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = varchar,
			     optional = true},
	      #riak_field_v1{name     = <<"erko">>,
			     position = 2,
			     type     = sint64,
			     optional = true},
	      #riak_field_v1{name     = <<"erkle">>,
			     position = 3,
			     type     = double,
			     optional = true},
	      #riak_field_v1{name     = <<"eejit">>,
			     position = 4,
			     type     = boolean,
			     optional = true},
	      #riak_field_v1{name     = <<"ergot">>,
			     position = 5,
			     type     = boolean,
			     optional = true},
	      #riak_field_v1{name     = <<"epithelion">>,
			     position = 6,
			     type     = set,
			     optional = true},
	      #riak_field_v1{name     = <<"endofdays">>,
			     position = 7,
			     type     = timestamp,
			     optional = true}
	     ]).

simple_valid_optional_3_test() ->
    DDL = make_simple_valid_optional_3_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({[], <<"edible">>}),
    ?assertEqual(?VALID, Result).

make_simple_valid_optional_3_ddl() ->
    make_ddl(<<"simple_valid_optional_3_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = varchar,
			     optional = true},
	      #riak_field_v1{name     = <<"yerk">>,
			     position = 2,
			     type     = varchar,
			     optional = false}
	     ]).

complex_valid_optional_1_test() ->
    DDL = make_complex_valid_optional_1_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({[], {1, []}}),
    ?assertEqual(?VALID, Result).

make_complex_valid_optional_1_ddl() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yando">>,
                                position = 1,
                                type     = sint64,
                                optional = true},
                 #riak_field_v1{name     = <<"yando">>,
                                position = 2,
                                type     = varchar,
                                optional = true}
                ]},
    _DDL = make_ddl(<<"complex_valid_optional_1_test">>,
		    [
		     #riak_field_v1{name     = <<"yando">>,
				    position = 1,
				    type     = varchar,
				    optional = true},
		     #riak_field_v1{name     = <<"yando">>,
				    position = 2,
				    type     = Map,
				    optional = false}
		    ]).

complex_valid_map_1_test() ->
    DDL = make_complex_valid_map_1_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({1, {1, {3, 4}, 1}, 1}),
    ?assertEqual(?VALID, Result).

make_complex_valid_map_1_ddl() ->
    Map2 = {map, [
                  #riak_field_v1{name     = <<"dingle">>,
                                 position = 1,
                                 type     = sint64},
                  #riak_field_v1{name     = <<"zoomer">>,
                                 position = 2,
                                 type     = sint64}
                 ]},
    Map1 = {map, [
                  #riak_field_v1{name     = <<"yarple">>,
                                 position = 1,
                                 type     = sint64},
                  #riak_field_v1{name     = <<"yoplait">>,
                                 position = 2,
                                 type     = Map2},
                  #riak_field_v1{name     = <<"yowl">>,
                                 position = 3,
                                 type     = sint64}
                 ]},
    _DDL = make_ddl(<<"complex_valid_map_1_test">>,
		    [
		     #riak_field_v1{name     = <<"yando">>,
				    position = 1,
				    type     = sint64},
		     #riak_field_v1{name     = <<"erko">>,
				    position = 2,
				    type     = Map1},
		     #riak_field_v1{name     = <<"erkle">>,
				    position = 3,
				    type     = sint64}
		    ]).

complex_valid_map_2_test() ->
    DDL = make_complex_valid_map_2_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({1, {1, {1, 1, {1, 1}}, 1}, 1}),
    ?assertEqual(?VALID, Result).

make_complex_valid_map_2_ddl() ->
    Map3 = {map, [
                  #riak_field_v1{name     = <<"yik">>,
                                 position = 1,
                                 type     = sint64},
                  #riak_field_v1{name     = <<"banjo">>,
                                 position = 2,
                                 type     = sint64}
                 ]},
    Map2 = {map, [
                  #riak_field_v1{name     = <<"dingle">>,
                                 position = 1,
                                 type     = sint64},
                  #riak_field_v1{name     = <<"zoomer">>,
                                 position = 2,
                                 type     = sint64},
                  #riak_field_v1{name     = <<"dougal">>,
                                 position = 3,
                                 type     = Map3}
                 ]},
    Map1 = {map, [
                  #riak_field_v1{name     = <<"yarple">>,
                                 position = 1,
                                 type     = sint64},
                  #riak_field_v1{name     = <<"yoplait">>,
                                 position = 2,
                                 type     = Map2},
                  #riak_field_v1{name     = <<"yowl">>,
                                 position = 3,
                                 type     = sint64}
                 ]},
    _DDL = make_ddl(<<"complex_valid_map_2_test">>,
		    [
		     #riak_field_v1{name     = <<"yando">>,
				    position = 1,
				    type     = sint64},
		     #riak_field_v1{name     = <<"erko">>,
				    position = 2,
				    type     = Map1},
		     #riak_field_v1{name     = <<"erkle">>,
				    position = 3,
				    type     = sint64}
		    ]).

complex_valid_map_3_test() ->
    DDL = make_complex_valid_map_3_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({{2, {3}, {4}}}),
    ?assertEqual(?VALID, Result).

make_complex_valid_map_3_ddl() ->
    Map3 = {map, [
                  #riak_field_v1{name     = <<"in_Map_2">>,
                                 position = 1,
                                 type     = sint64}
                 ]},
    Map2 = {map, [
                  #riak_field_v1{name     = <<"in_Map_1">>,
                                 position = 1,
                                 type     = sint64}
                 ]},
    Map1 = {map, [
                  #riak_field_v1{name     = <<"Level_1_1">>,
                                 position = 1,
                                 type     = sint64},
                  #riak_field_v1{name     = <<"Level_1_map1">>,
                                 position = 2,
                                 type     = Map2},
                  #riak_field_v1{name     = <<"Level_1_map2">>,
                                 position = 3,
                                 type     = Map3}
                 ]},
    _DDL = make_ddl(<<"complex_valid_map_3_test">>,
		    [
		     #riak_field_v1{name     = <<"Top_Map">>,
				    position = 1,
				    type     = Map1}
		    ]).

simple_valid_any_test() ->
    DDL = make_simple_valid_any_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, [a, b, d], 4.4}),
    ?assertEqual(?VALID, Result).

make_simple_valid_any_ddl() ->
    make_ddl(<<"simple_valid_any_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = varchar},
	      #riak_field_v1{name     = <<"erko">>,
			     position = 2,
			     type     = any},
	      #riak_field_v1{name     = <<"erkle">>,
			     position = 3,
			     type     = double}
	     ]).

simple_valid_set_test() ->
    DDL = make_simple_valid_set_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, [a, b, d], 4.4}),
    ?assertEqual(?VALID, Result).

make_simple_valid_set_ddl() ->
    make_ddl(<<"simple_valid_set_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = varchar},
	      #riak_field_v1{name     = <<"erko">>,
			     position = 2,
			     type     = set},
	      #riak_field_v1{name     = <<"erkle">>,
			     position = 3,
			     type     = double}
	     ]).

simple_valid_mixed_test() ->
    DDL = make_simple_valid_mixed_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, 99, 4.4, 5555}),
    ?assertEqual(?VALID, Result).

make_simple_valid_mixed_ddl() ->
    make_ddl(<<"simple_valid_mixed_test">>,
	     [
	      #riak_field_v1{name     = <<"yando">>,
			     position = 1,
			     type     = varchar},
	      #riak_field_v1{name     = <<"erko">>,
			     position = 2,
			     type     = sint64},
	      #riak_field_v1{name     = <<"erkle">>,
			     position = 3,
			     type     = double},
	      #riak_field_v1{name     = <<"banjo">>,
			     position = 4,
			     type     = timestamp}
	     ]).

%%
%% invalid tests
%%

simple_invalid_varchar_test() ->
    DDL = make_ddl(<<"simple_invalid_varchar_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = varchar}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, 55}),
    ?assertEqual(?INVALID, Result).

simple_invalid_sint64_test() ->
    DDL = make_ddl(<<"simple_invalid_sint64_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = sint64},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = sint64},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = sint64}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({999, -9999, 0,0}),
    ?assertEqual(?INVALID, Result).

simple_invalid_double_test() ->
    DDL = make_ddl(<<"simple_invalid_double_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = double},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = double},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({432.22, -23423.22, [a, b, d]}),
    ?assertEqual(?INVALID, Result).

simple_invalid_boolean_test() ->
    DDL = make_ddl(<<"simple_invalid_boolean_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = boolean},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = boolean},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = boolean}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({true, false, [a, b, d]}),
    ?assertEqual(?INVALID, Result).

simple_invalid_set_test() ->
    DDL = make_ddl(<<"simple_invalid_set_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = set},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, [444.44], 4.4}),
    ?assertEqual(?VALID, Result).

simple_invalid_timestamp_1_test() ->
    DDL = make_ddl(<<"simple_invalid_timestamp_1_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = timestamp},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = timestamp},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = timestamp}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({234.324, 23424, 34636}),
    ?assertEqual(?INVALID, Result).

simple_invalid_timestamp_2_test() ->
    DDL = make_ddl(<<"simple_invalid_timestamp_2_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = timestamp},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = timestamp},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = timestamp}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({234324, -23424, 34636}),
    ?assertEqual(?INVALID, Result).

simple_invalid_timestamp_3_test() ->
    DDL = make_ddl(<<"simple_invalid_timestamp_3_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = timestamp},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = timestamp},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = timestamp}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({234324, 0, 34636}),
    ?assertEqual(?INVALID, Result).

simple_invalid_map_1_test() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yarple">>,
                                position = 1,
                                type     = varchar}
                ]},
    DDL = make_ddl(<<"simple_invalid_map_1_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = Map},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, {99}, 4.4}),
    ?assertEqual(?INVALID, Result).

simple_invalid_map_2_test() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yarple">>,
                                position = 1,
                                type     = varchar},
                 #riak_field_v1{name     = <<"yip">>,
                                position = 2,
                                type     = varchar}
                ]},
    DDL = make_ddl(<<"simple_invalid_map_2_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = Map},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, {<<"erer">>}, 4.4}),
    ?assertEqual(?INVALID, Result).

simple_invalid_map_3_test() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yarple">>,
                                position = 1,
                                type     = varchar},
                 #riak_field_v1{name     = <<"yip">>,
                                position = 2,
                                type     = varchar}
                ]},
    DDL = make_ddl(<<"simple_invalid_map_3_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = Map},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, {<<"bingo">>, <<"bango">>, <<"erk">>}, 4.4}),
    ?assertEqual(?INVALID, Result).

simple_invalid_map_4_test() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yarple">>,
                                position = 1,
                                type     = varchar}
                ]},
    DDL = make_ddl(<<"simple_invalid_map_4_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = Map},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>, [99], 4.4}),
    ?assertEqual(?INVALID, Result).

complex_invalid_map_1_test() ->
    Map2 = {map, [
                  #riak_field_v1{name     = <<"dingle">>,
                                 position = 1,
                                 type     = varchar},
                  #riak_field_v1{name     = <<"zoomer">>,
                                 position = 2,
                                 type     = sint64}
                 ]},
    Map1 = {map, [
                  #riak_field_v1{name     = <<"yarple">>,
                                 position = 1,
                                 type     = varchar},
                  #riak_field_v1{name     = <<"yoplait">>,
                                 position = 2,
                                 type     = Map2},
                  #riak_field_v1{name     = <<"yowl">>,
                                 position = 3,
                                 type     = sint64}
                 ]},
    DDL = make_ddl(<<"complex_invalid_map_1_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = Map1},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({<<"ewrewr">>,
				  {<<"erko">>,
				   {<<"yerk">>, 33.0}
				  },
				  4.4}),
    ?assertEqual(?INVALID, Result).

%%% test the size of the tuples
too_small_tuple_test() ->
    DDL = make_ddl(<<"simple_too_small_tuple_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = double},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = double},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({432.22, -23423.22}),
    ?assertEqual(?INVALID, Result).

too_big_tuple_test() ->
    DDL = make_ddl(<<"simple_too_big_tuple_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = double},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = double},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({432.22, -23423.22, 44.44, 65.43}),
    ?assertEqual(?INVALID, Result).

%%
%% Extract tests
%%

simplest_valid_extract_test() ->
    DDL = make_ddl(<<"simplest_valid_extract_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Obj = {<<"yarble">>},
    Result = (catch Module:extract(Obj, [<<"yando">>])),
    ?assertEqual(<<"yarble">>, Result).

simple_valid_extract_test() ->
    DDL = make_ddl(<<"simple_valid_extract_test">>,
                   [
                    #riak_field_v1{name     = <<"scoobie">>,
                                   position = 1,
                                   type     = sint64},
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 2,
                                   type     = varchar}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Obj = {44, <<"yarble">>},
    Result = (catch Module:extract(Obj, [<<"yando">>])),
    ?assertEqual(<<"yarble">>, Result).

simple_valid_map_extract_test() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yarple">>,
                                position = 1,
                                type     = varchar}
                ]},
    DDL = make_ddl(<<"simple_valid_map_extract_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
                    #riak_field_v1{name     = <<"erko">>,
                                   position = 2,
                                   type     = Map},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Obj = {<<"erk">>, {<<"jibjib">>}, 3.0},
    Result = (catch Module:extract(Obj, [<<"erko">>, <<"yarple">>])),
    ?assertEqual(<<"jibjib">>, Result).

complex_valid_map_extract_test() ->
    Map3 = {map, [
                  #riak_field_v1{name     = <<"in_Map_2">>,
                                 position = 1,
                                 type     = sint64}
                 ]},
    Map2 = {map, [
                  #riak_field_v1{name     = <<"in_Map_1">>,
                                 position = 1,
                                 type     = sint64}
                 ]},
    Map1 = {map, [
                  #riak_field_v1{name     = <<"Level_1_1">>,
                                 position = 1,
                                 type     = sint64},
                  #riak_field_v1{name     = <<"Level_1_map1">>,
                                 position = 2,
                                 type     = Map2},
                  #riak_field_v1{name     = <<"Level_1_map2">>,
                                 position = 3,
                                 type     = Map3}
                 ]},
    DDL = make_ddl(<<"complex_valid_map_extract_test">>,
                   [
                    #riak_field_v1{name     = <<"Top_Map">>,
                                   position = 1,
                                   type     = Map1}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Obj = {{2, {3}, {4}}},
    Path = [<<"Top_Map">>, <<"Level_1_map1">>, <<"in_Map_1">>],
    Res = (catch Module:extract(Obj, Path)),
    ?assertEqual(3, Res).

complex_ddl_test() ->
    DDL = make_complex_ddl_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({12345, <<"beeees">>}),
    ?assertEqual(?VALID, Result).

make_complex_ddl_ddl() ->
    #ddl_v1{
       table = <<"temperatures">>,
       fields = [
		 #riak_field_v1{
		    name     = <<"time">>,
		    position = 1,
		    type     = timestamp,
		    optional = false},
		 #riak_field_v1{
		    name     = <<"user_id">>,
		    position = 2,
		    type     = varchar,
		    optional = false}
		],
       partition_key = #key_v1{ast = [
				      #param_v1{name = [<<"time">>]},
				      #hash_fn_v1{mod  = crypto,
						  fn   = hash,
						  %% list isn't a valid arg
						  %% type output from the
						  %% lexer/parser
						  args = [
							  sha512,
							  true,
							  1,
							  1.0,
							  <<"abc">>
							 ]}
				     ]},
       local_key = #key_v1{ast = [
				  #hash_fn_v1{mod  = crypto,
					      fn   = hash,
					      args = [ripemd]},
				  #param_v1{name = [<<"time">>]}
				 ]}}.

%%
%% Test the add column info functionality
%%

simple_valid_map_add_cols_test() ->
    Map = {map, [
                 #riak_field_v1{name     = <<"yarple">>,
                                position = 1,
                                type     = varchar},
                 #riak_field_v1{name     = <<"yoplait">>,
                                position = 2,
                                type     = sint64}
                ]},
    DDL = make_ddl(<<"simple_valid_map_2_test">>,
                   [
                    #riak_field_v1{name     = <<"yando">>,
                                   position = 1,
                                   type     = varchar},
		    #riak_field_v1{name     = <<"erko">>,
				   position = 2,
                                   type     = Map},
                    #riak_field_v1{name     = <<"erkle">>,
                                   position = 3,
                                   type     = double}
                   ]),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:add_column_info({1, {2, 3}, 4}),
    Expected = [{<<"yando">>, 1}, {<<"erko">>, [
						{<<"yarple">>, 2},
						{<<"yoplait">>, 3}
					       ]},
		{<<"erkle">>, 4}],
    ?assertEqual(Expected, Result).

%%
%% test the get_ddl function
%%

-define(ddl_roundtrip_assert(MakeDDLFunName),
	DDL = erlang:apply(?MODULE, MakeDDLFunName, []),
	{module, Module} = compile_and_load_from_tmp(DDL),
	Got = Module:get_ddl(),
	?assertEqual(DDL, Got)).

simplest_valid_get_test() ->
    ?ddl_roundtrip_assert(make_simplest_valid_ddl).

simple_valid_varchar_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_varchar_ddl).

simple_valid_sint64_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_sint64_ddl).

simple_valid_double_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_double_ddl).

simple_valid_boolean_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_boolean_ddl).

simple_valid_timestamp_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_timestamp_ddl).

simple_valid_map_1_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_map_1_ddl).

simple_valid_map_2_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_map_2_ddl).

simple_valid_optional_1_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_optional_1_ddl).

simple_valid_optional_2_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_optional_2_ddl).

simple_valid_optional_3_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_optional_3_ddl).

complex_valid_optional_3_get_test() ->
    ?ddl_roundtrip_assert(make_complex_valid_optional_1_ddl).

complex_valid_map_1_get_test() ->
    ?ddl_roundtrip_assert(make_complex_valid_map_1_ddl).

complex_valid_map_2_get_test() ->
    ?ddl_roundtrip_assert(make_complex_valid_map_2_ddl).

complex_valid_map_3_get_test() ->
    ?ddl_roundtrip_assert(make_complex_valid_map_3_ddl).

simple_valid_any_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_any_ddl).

simple_valid_set_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_set_ddl).

simple_valid_mixed_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_mixed_ddl).

complex_ddl_get_test() ->
    ?ddl_roundtrip_assert(make_complex_ddl_ddl).

timeseries_ddl_get_test() ->
    ?ddl_roundtrip_assert(make_timeseries_ddl).

make_timeseries_ddl() ->
    Fields = [
	      #riak_field_v1{name     = <<"geohash">>,
			     position = 1,
			     type     = varchar,
			     optional = false},
	      #riak_field_v1{name     = <<"user">>,
			     position = 2,
			     type     = varchar,
			     optional = false},
	      #riak_field_v1{name     = <<"time">>,
			     position = 3,
			     type     = timestamp,
			     optional = false},
	      #riak_field_v1{name     = <<"weather">>,
			     position = 4,
			     type     = varchar,
			     optional = false},
	      #riak_field_v1{name     = <<"temperature">>,
			     position = 5,
			     type     = varchar,
			     optional = true}
	     ],
    PK = #key_v1{ ast = [
			 #hash_fn_v1{mod  = riak_ql_quanta,
				     fn   = quantum,
				     args = [
					     #param_v1{name = [<<"time">>]},
					     15,
					     s
					    ]}
			]},
    LK = #key_v1{ast = [
			#param_v1{name = [<<"time">>]},
			#param_v1{name = [<<"user">>]}]
		},
    _DDL = #ddl_v1{table         = <<"timeseries_filter_test">>,
		   fields        = Fields,
		   partition_key = PK,
		   local_key     = LK
		  }.

-endif.
