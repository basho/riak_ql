%% -------------------------------------------------------------------
%%
%% riak_ql_ddl_compiler: processes the Data Description in the DDL
%% * verifies that the DDL is valid
%% * compiles the record description in the DDL into a module that can
%%   be used to verify that incoming data conforms to a schema at the boundary
%%
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
-include_lib("merl/include/merl.hrl").

-export([
         compile/1,
         compile_and_load_disabled_module_from_tmp/1,
         compile_and_load_from_tmp/1,
         compile_disabled_module/1,
         get_compiler_capabilities/0,
         get_compiler_version/0,
         write_source_to_files/3
        ]).

-define(IGNORE,     true).
-define(DONTIGNORE, false).
-define(NOPREFIX,   []).

%% you have to start line and positions nos somewhere
-define(POSNOSTART,  1).
-define(LINENOSTART, 1).
-define(ARITYOF1,    1).

-type expr()   :: tuple().
-type exprs()  :: [expr()].
-type guards() :: [exprs()].
-type ast()    :: [expr() | exprs() | guards()].

%% MD5 checksum of this module.
-spec get_compiler_version() -> integer().
get_compiler_version() ->
    Attributes = module_info(attributes),
    {vsn, [Vsn]} = lists:keyfind(vsn, 1, Attributes),
    Vsn.

%% Create a list of supported versions from the current one
%% down to the original version 1
-spec get_compiler_capabilities() -> [riak_ql_component:component_version()].
get_compiler_capabilities() ->
    lists:seq(get_compiler_version(), 1, -1).

%% Return the AST for a disabled module, a module can be disabled when a DDL
%% is not available for a table because it has been downgraded to a version
%% that is lower than what the table features required.
-spec compile_disabled_module(Table::binary()) -> {module(), tuple()}.
compile_disabled_module(Table) when is_binary(Table) ->
    ModuleName = riak_ql_ddl:make_module_name(Table),
    {ModuleName, merl:quote(disabled_table_source(ModuleName))}.

%%
disabled_table_source(ModuleName) ->
    "-module(" ++ atom_to_list(ModuleName) ++ ").\n"
    "-compile(export_all).\n"
    "get_min_required_ddl_cap() -> disabled.".

%% Compile the DDL to its helper module AST.
-spec compile(?DDL{}) ->
    {module(), ast()} | {error, tuple()}.
compile({ok, ?DDL{} = DDL}) ->
    %% handle output directly from riak_ql_parser
    compile(DDL);
compile(?DDL{ table = Table, fields = Fields } = DDL) ->
    {ModName, Attrs,   LineNo} = make_attrs(Table, ?LINENOSTART),
    {VFns,             LineNo2}  = build_validn_fns(Fields,    LineNo),
    {ACFns,            LineNo3}  = build_add_cols_fns(Fields,  LineNo2),
    {ExtractFn,        LineNo4}  = build_extract_fn(DDL,  LineNo3),
    {GetTypeFn,        LineNo5}  = build_get_type_fn([Fields], LineNo4, []),    
    {GetPosnFn,        LineNo6}  = build_get_posn_fn(Fields,   LineNo5, []),
    {GetPosnsFn,       LineNo7}  = build_get_posns_fn(Fields,  LineNo6, []),
    {IsValidFn,        LineNo8}  = build_is_valid_fn(Fields,   LineNo7),
    {DDLVersionFn,     LineNo9}  = build_get_ddl_compiler_version_fn(LineNo8),
    {GetDDLFn,         LineNo10} = build_get_ddl_fn(DDL, LineNo9, []),
    {HashFns,          LineNo11} = build_identity_hash_fns(DDL, LineNo10),
    {FieldOrdersFn,    LineNo10} = build_field_orders_fn(DDL, LineNo9),
    {RevertOrderingFn, LineNo11} = build_revert_ordering_on_local_key_fn(DDL, LineNo10),
    {MinDDLCapFn,      LineNo12} = build_min_ddl_version_fn(DDL, LineNo11),
    {DeleteKeyFn,      LineNo11} = build_delete_key_fn(DDL, LineNo10, []),
    AST = Attrs
        ++ VFns
        ++ ACFns
        ++ [ExtractFn, GetTypeFn, GetPosnFn, GetPosnsFn, IsValidFn, DDLVersionFn, 
            GetDDLFn, FieldOrdersFn, RevertOrderingFn, MinDDLCapFn, DeleteKeyFn]
        ++ HashFns
        ++ [{eof, LineNo12}],
    case erl_lint:module(AST) of
        {ok, []} ->
            {ModName, AST};
        Other ->
            exit(Other)
    end.

%%
build_min_ddl_version_fn(DDL, LineNo) ->
    MinDDLVersion = riak_ql_ddl:get_minimum_capability(DDL),
    Source =
        "get_min_required_ddl_cap() -> " ++ atom_to_list(MinDDLVersion) ++ ".",
    {?Q(Source), LineNo+1}.


%% Compile the module, write it to /tmp then load it into the VM.
-spec compile_and_load_from_tmp(?DDL{}) ->
                                       {module, atom()} | {error, tuple()}.
compile_and_load_from_tmp({ok, {?DDL{} = DDL, _Props}}) ->
    %% handle output directly from riak_ql_parser
    compile_and_load_from_tmp(DDL);
compile_and_load_from_tmp(?DDL{} = DDL) ->
    {Module, AST} = compile(DDL),
    {ok, Module, Bin} = compile:forms(AST),
    BeamFileName = "/tmp/" ++ atom_to_list(Module) ++ ".beam",
    {module, Module} = code:load_binary(Module, BeamFileName, Bin).

%%
-spec compile_and_load_disabled_module_from_tmp(Table::binary()) -> {module, module()}.
compile_and_load_disabled_module_from_tmp(Table) when is_binary(Table) ->
    {Module, AST} = compile_disabled_module(Table),
    {ok, Module, Bin} = compile:forms(AST),
    BeamFileName = "/tmp/" ++ atom_to_list(Module) ++ ".beam",
    {module, Module} = code:load_binary(Module, BeamFileName, Bin).

%% Write the AST and erlang source derived from the AST to disk for debugging.
-spec write_source_to_files(string(), ?DDL{}, ast()) -> ok.
write_source_to_files(OutputDir, ?DDL{ table = Table } = DDL, AST) ->
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
build_debug_header_text(?DDL{table = Table, fields = Fields,
                             partition_key = PartitionKey,
                             local_key = LocalKey}) ->
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

-spec build_identity_hash_fns(?DDL{}, pos_integer()) ->
             {[expr()], pos_integer()}.
build_identity_hash_fns(?DDL{} = DDL, LineNo) ->
    PlainText = make_plain_text(DDL),
    Hash = crypto:hash(md5, PlainText),
    %% we return a list of hashes - this means that going forward
    %% we can offer a remote cluster a set of valid hashes that it can use
    %% so that down-compatible DDLs etc can be replicated
    HashFn  = flat_format("get_identity_hashes() -> [~p].", [Hash]),
    DebugFn = flat_format("get_identity_plaintext_DEBUG() -> ~s.", [PlainText]),
    {[?Q(HashFn), ?Q(DebugFn)], LineNo + 1}.

%% not using DDL macro because this will be version specific
make_plain_text(?DDL{table         = Table,
                     fields        = Fields,
                     partition_key = PK,
                     local_key     = LK}) ->
   string:join(["\"TABLE"]
               ++ io_lib:format("~s", [Table])
               ++ ["FIELDS"]
               ++ canonical_fields(Fields)
               ++ ["PRIMARY KEY"]
               ++ canonical_key(PK)
               ++ ["LOCAL KEY"]
               ++  canonical_key(LK)
              , ": ") ++ ["\""].

canonical_fields(Fields) ->
    F2 = lists:keysort(#riak_field_v1.position, Fields),
    [canonical_field_format(binary_to_list(X#riak_field_v1.name),
                         X#riak_field_v1.type,
                         X#riak_field_v1.optional) || X <- F2].

canonical_field_format(Nm, Ty, true)  -> 
    Msg = io_lib:format("~s ~s", [Nm, Ty]),
    lists:flatten(Msg);
canonical_field_format(Nm, Ty, false) -> 
    Msg = io_lib:format("~s ~s not null", [Nm, Ty]),
    lists:flatten(Msg).

canonical_key(#key_v1{ast = AST}) ->
    [canonical_ast_fmt(X) || X <- AST].

canonical_ast_fmt(#hash_fn_v1{mod  = Mod,
                              fn   = Fn,
                              args = Args,
                              type = Type}) ->
    Args2 = [canonical_arg_fmt_v1(X) || X <- Args],
    string:join([atom_to_list(Mod), atom_to_list(Fn)]
                        ++ Args2 ++ [atom_to_list(Type)], " ");
canonical_ast_fmt(?SQL_PARAM{name = [Nm]}) ->
    binary_to_list(Nm).

canonical_arg_fmt_v1(B) when is_binary(B)    -> binary_to_list(B);
canonical_arg_fmt_v1(F) when is_float(F)     -> float_to_list(F);
canonical_arg_fmt_v1(N) when is_integer(N)   -> integer_to_list(N);
canonical_arg_fmt_v1(A) when is_atom(A)      -> atom_to_list(A);
canonical_arg_fmt_v1(?SQL_PARAM{name = [Nm]}) -> binary_to_list(Nm).

%% Build the table helper function to extract the value of a cell from a row,
%% using the field name.
build_extract_fn(DDL, LineNo) ->
    {?Q(build_extract_fn_source(DDL)), LineNo+1}.

build_extract_fn_source(?DDL{fields = Fields}) ->
    {Clauses, _} = lists:mapfoldl(
        fun(Field, IndexAcc) ->
            Clause = lists:flatten(build_extract_fn_clause_source(Field, IndexAcc)),
            {Clause, IndexAcc+1}
        end, 1, Fields),
    string:join(Clauses, ";\n") ++ ".".

build_extract_fn_clause_source(#riak_field_v1{name = Name}, Index) ->
    io_lib:format(
        "extract(Obj, [~p]) when is_tuple(Obj) -> element(~p, Obj)",
        [Name, Index]).

-spec build_get_ddl_compiler_version_fn(LineNo :: pos_integer()) ->
                                               {expr(), pos_integer()}.
build_get_ddl_compiler_version_fn(LineNo) ->
    Fn = flat_format("get_ddl_compiler_version() -> ~b.",
                     [?RIAK_QL_DDL_COMPILER_VERSION]),
    {?Q(Fn), LineNo + 1}.

-spec build_get_ddl_fn(?DDL{}, pos_integer(), ast()) ->
                              {expr(), pos_integer()}.
build_get_ddl_fn(DDL, LineNo, []) ->
    Fn = flat_format("get_ddl() -> ~p.", [DDL]),
    {?Q(Fn), LineNo + 1}.

build_delete_key_fn(DDL, LineNo, []) ->
    Fn = flat_format("get_delete_key(W) ->"
                     "LocalKey = ~p, "
                     "case riak_ql_ddl_util:is_valid_delete_where_clause(W) of "
                     "true -> riak_ql_ddl_util:make_delete_key(LocalKey, W); "
                     "{error, Errors} -> {false, Errors} "
                     "end.",
                     [DDL?DDL.local_key]),
    {?Q(Fn), LineNo + 1}.               

%% Build the AST for a function returning a list of the order
%% of the table field orders
%%     `field_orders() -> [ascending, descending, ascending].'
build_field_orders_fn(DDL, LineNum) ->
    {?Q(build_field_orders_fn_string(DDL)), LineNum+1}.

%%
build_field_orders_fn_string(?DDL{ local_key = #key_v1{ ast = AST } }) ->
    Orders = [order_from_key_field(F) || F <- AST],
    lists:flatten(io_lib:format("field_orders() -> ~p.",[Orders])).

%%
order_from_key_field(?SQL_PARAM{ ordering = undefined }) ->
    ascending;
order_from_key_field(?SQL_PARAM{ ordering = Ordering }) ->
    Ordering;
order_from_key_field(_) ->
    ascending.

%% Convert a local key that has had the descending logic run on it to it's
%% original value. Used to convert streamed list keys back to their original
%% values.
build_revert_ordering_on_local_key_fn(DDL, LineNum) ->
    {?Q(build_revert_ordering_on_local_key_fn_string(DDL)), LineNum+1}.

%%
build_revert_ordering_on_local_key_fn_string(?DDL{ local_key = #key_v1{ ast = AST } } = DDL) ->
    FieldNameArgs =
        string:join([to_var_name_string(N) || ?SQL_PARAM{ name = N } <- AST], ","),
    Results =
        string:join([revert_ordering_on_local_key_element(DDL, P) || P <- AST], ","),
    lists:flatten(io_lib:format(
        "revert_ordering_on_local_key({~s}) -> [~s].",[FieldNameArgs, Results])).

%%
revert_ordering_on_local_key_element(DDL, ?SQL_PARAM{ name = N1, ordering = descending }) ->
    N2 = to_var_name_string(N1),
    case field_type(DDL, hd(N1)) of
        varchar ->
            "riak_ql_ddl:flip_binary(" ++ N2 ++ ")";
        Type when Type == timestamp; Type == sint64 ->
            %% in the case of integers we just negate the original negation
            N2 ++ "*-1";
        _ ->
            N2
    end;
revert_ordering_on_local_key_element(_, ?SQL_PARAM{ name = N }) ->
    to_var_name_string(N).

%%
to_var_name_string([FieldName]) ->
    to_var_name_string(FieldName);
to_var_name_string(FieldName) when is_binary(FieldName) ->
    [H|Tail] = binary_to_list(FieldName),
    [string:to_upper([H]) | Tail].

%%
field_type(?DDL{ fields = Fields }, FieldName) ->
    #riak_field_v1{ type = Type } =
        lists:keyfind(FieldName, #riak_field_v1.name, Fields),
    Type.

-spec build_is_valid_fn([#riak_field_v1{}], pos_integer()) ->
                               {expr(), pos_integer()}.
build_is_valid_fn(Fields, LineNo) ->
    {Fns, LineNo2} = make_is_valid_cls(Fields, LineNo, []),
    {Fail, LineNo3} = make_fail_clause(LineNo2),
    Clauses = lists:flatten(lists:reverse([Fail | Fns])),
    Fn = make_fun(is_field_valid, 1, Clauses, LineNo),
    {Fn, LineNo3}.

make_is_valid_cls([], LineNo, Acc) ->
    %% handle the prefixes slightly differently here than to the next clause
    WildArgs = [make_string("*", LineNo)],
    WildConses = make_conses(WildArgs, LineNo, {nil, LineNo}),
    Body = make_atom(true, LineNo),
    Guard = [],
    WildCl = make_clause([WildConses], Guard, Body, LineNo),
    {lists:reverse([WildCl | Acc]), LineNo};
make_is_valid_cls([#riak_field_v1{name = Nm} | T], LineNo, Acc) ->
    %% handle the prefixes slightly differently here than to the previous clause
    Args   = [make_string(binary_to_list(Nm), LineNo)],
    Conses = make_conses(Args, LineNo, {nil, LineNo}),
    %% you need to reverse the lists of the positions to
    %% get the calls to element to nest correctly
    Body = make_atom(true, LineNo),
    Guard = [],
    Cl = make_clause([Conses], Guard, Body, LineNo),
    {NewA, NewLineNo} = {[Cl | Acc], LineNo},
    make_is_valid_cls(T, NewLineNo, NewA).

%% the funny shape of this function is because it used to handle maps
%% which needed recursion - not refactoring because it will be merled
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
    Body = make_atom(Ty, LineNo),
    Guard = [],
    Cl = make_clause([Conses], Guard, Body, LineNo),
    {NewA, NewLineNo} = {[Cl | Acc], LineNo},
    make_get_type_cls(T, NewLineNo, Prefix, NewA).

-spec build_get_posn_fn([#riak_field_v1{}], pos_integer(), ast()) ->
                               {expr(), pos_integer()}.
build_get_posn_fn([], LineNo, Acc) ->
    Fn = Acc ++ "get_field_position(_Other) -> undefined.",
    {?Q(Fn), LineNo + 1};

build_get_posn_fn([#riak_field_v1{name = Name, position = Pos} | T], LineNo, Acc) ->
    Clause = flat_format("get_field_position([<<\"~s\">>]) -> ~p;", [Name, Pos]),
    build_get_posn_fn(T, LineNo, Acc ++ Clause).

-spec build_get_posns_fn([#riak_field_v1{}], pos_integer(), ast()) ->
                                {expr(), pos_integer()}.
build_get_posns_fn([], LineNo, Acc) ->
    Header = "get_field_positions() -> [",
    Body = string:strip(Acc, right, $,),
    End = "].",
    Fn = flat_format("~s~s~s", [Header, Body, End]),
    {?Q(Fn), LineNo + 1};

build_get_posns_fn([#riak_field_v1{name = Name, position = Pos} | T], LineNo, Acc) ->
    Clause = flat_format("{[<<\"~s\">>], ~p},", [Name, Pos]),
    build_get_posns_fn(T, LineNo, Acc ++ Clause).

make_conses([], _LineNo, Conses)  -> Conses;
make_conses([H | T], LineNo, Acc) -> NewAcc = {cons, LineNo, H, Acc},
                                     make_conses(T, LineNo, NewAcc).

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
make_ar2([#riak_field_v1{name = Nm} = F | T], LineNo, Acc1, Acc2) ->
    [NewAcc1] = make_names([F], LineNo),
    NewAcc2 = make_tuple([make_string(binary_to_list(Nm), LineNo), NewAcc1], LineNo),
    make_ar2(T, LineNo, [NewAcc1 | Acc1], [NewAcc2 | Acc2]).

build_validn_fns(Fields, LineNo) ->
    Fn = "validate_obj",
    Names = [escape(Name) || {_, _, Name} <- make_names(Fields, LineNo)],
    Params = [{Rec#riak_field_v1.type, Rec#riak_field_v1.optional} || Rec <- Fields],
    FunCall = io_lib:format(Fn ++ "({" ++ string:join(Names, ", ") ++ "}) when ", []),
    Guards = [make_guard(Variable, Type, Optional) || {Variable, {Type, Optional}} <- lists:zip(Names, Params)],
    Guards2 = [X || X <- Guards, X =/= []],
    Guards3 = string:join(Guards2, ", "),
    SuccessClause = FunCall ++ Guards3 ++ " -> true;",
    FailClause = Fn ++ "(_) -> false.",
    ValidationFun = SuccessClause ++ FailClause,
    {[?Q(ValidationFun)], LineNo}.

escape(Binary) ->
    List = atom_to_list(Binary),
    lists:filter(fun is_alphanumeric/1, List).

is_alphanumeric(N) when (N >= $0 andalso N =< $9) orelse
                        (N >= $a andalso N =< $z) orelse
                        (N >= $A andalso N =< $Z) orelse
                        N =:= $_ -> true;
is_alphanumeric(_N)              -> false.

make_guard(Variable, varchar, true) ->
    Variable ++ " =:= [] orelse is_binary(" ++ Variable ++ ")";
make_guard(Variable, varchar, false) ->
    "is_binary(" ++ Variable ++ ")";
make_guard(Variable, sint64, true) ->
    Variable ++ " =:= [] orelse is_integer(" ++ Variable ++ ")";
make_guard(Variable, sint64, false) ->
    "is_integer(" ++ Variable ++ ")";
make_guard(Variable, double, true) ->
    Variable ++ " =:= [] orelse is_float(" ++ Variable ++ ")";
make_guard(Variable, double, false) ->
    "is_float(" ++ Variable ++ ")";
make_guard(Variable, boolean, true) ->
    Variable ++ " =:= [] orelse is_boolean(" ++ Variable ++ ")";
make_guard(Variable, boolean, false) ->
    "is_boolean(" ++ Variable ++ ")";
make_guard(Variable, set, true) ->
    Variable ++ " =:= [] orelse is_list(" ++ Variable ++ ")";
make_guard(Variable, set, false) ->
    "is_list(" ++ Variable ++ ")";
make_guard(Variable, timestamp, true) ->
    Variable ++ " =:= [] orelse (is_integer(" ++ Variable ++ ") andalso " ++ Variable ++ " > 0)";
make_guard(Variable, timestamp, false) ->
    "is_integer(" ++ Variable ++ ") andalso " ++ Variable ++ " > 0".

-spec make_attrs(binary(), pos_integer()) -> {atom(), ast(), pos_integer()}.
make_attrs(Bucket, LineNo) when is_binary(Bucket)    ->
    ModName = riak_ql_ddl:make_module_name(Bucket),
    {ModAttr, LineNo1} = make_module_attr(ModName, LineNo),
    {ExpAttr, LineNo2} = make_export_attr(LineNo1),
    {ModName, [ModAttr, ExpAttr], LineNo2}.

-spec make_fun(atom(), 0 | pos_integer(), ast(), pos_integer()) -> expr().
make_fun(FunName, Arity, Clause, LineNo) ->
    {function, LineNo, FunName, Arity, Clause}.

-spec make_atom(atom(), pos_integer()) -> expr().
make_atom(A, LineNo) when is_atom(A) -> {atom, LineNo, A}.

-spec make_names([#riak_field_v1{}], pos_integer()) -> [expr()].
make_names(Fields, LineNo) ->
    Make_fn = fun(#riak_field_v1{name     = Name,
                                 position = NPos}) ->
                      make_name(Name, LineNo, NPos)
              end,
    [Make_fn(X) || X <- Fields].

-spec make_name(binary(), pos_integer(), pos_integer()) -> expr().
make_name(Name,  LineNo, NPos) ->
    Nm = binary_to_atom(iolist_to_binary(["Var", integer_to_list(NPos), "_", Name]), utf8),
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
                                  {add_column_info,              1},
                                  {extract,                      2},
                                  {field_orders,                 0},
                                  {get_ddl,                      0},
                                  {get_ddl_compiler_version,     0},
                                  {get_field_position,           1},
                                  {get_field_positions,          0},
                                  {get_field_type,               1},
                                  {get_identity_hashes,          0},
                                  {get_identity_plaintext_DEBUG, 0},
                                  {is_field_valid,               1},
                                  {revert_ordering_on_local_key, 1},
                                  {validate_obj,                 1},
                                  {get_min_required_ddl_cap,     0},
                                  {get_delete_key,               1}
                                 ]}, LineNo + 1}.

%% supporting functions

flat_format(Format, Args) ->
    lists:flatten(io_lib:format(Format, Args)).

-ifdef(TEST).
-compile(export_all).

-define(VALID,   true).
-define(INVALID, false).

-include_lib("eunit/include/eunit.hrl").

%%
%% Helper fns
%%

make_ddl(?DDL{table = Table,
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
    ?DDL{table         = Table,
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

complex_ddl_test() ->
    DDL = make_complex_ddl_ddl(),
    {module, Module} = compile_and_load_from_tmp(DDL),
    Result = Module:validate_obj({12345, <<"beeees">>}),
    ?assertEqual(?VALID, Result).

make_complex_ddl_ddl() ->
    Table_def =
        "CREATE TABLE temperatures ("
        "time    TIMESTAMP NOT NULL, "
        "user_id VARCHAR NOT NULL, "
        "PRIMARY KEY ((user_id, quantum(time, 15, 's')), user_id, time))",
    {ddl, DDL, _} =
        riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(Table_def)),
    DDL.

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

simple_valid_optional_1_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_optional_1_ddl).

simple_valid_optional_2_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_optional_2_ddl).

simple_valid_optional_3_get_test() ->
    ?ddl_roundtrip_assert(make_simple_valid_optional_3_ddl).

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
                        ?SQL_PARAM{name = [<<"time">>]},
                        ?SQL_PARAM{name = [<<"user">>]},
                        #hash_fn_v1{mod  = riak_ql_quanta,
                                     fn   = quantum,
                                     args = [
                                             ?SQL_PARAM{name = [<<"time">>]},
                                             15,
                                             s
                                            ],
                                   type = timestamp}
                        ]},
    LK = #key_v1{ast = [
                        ?SQL_PARAM{name = [<<"time">>]},
                        ?SQL_PARAM{name = [<<"user">>]}]
                },
    _DDL = ?DDL{table         = <<"timeseries_filter_test">>,
                fields        = Fields,
                partition_key = PK,
                local_key     = LK
               }.

build_field_orders_fn_string_test() ->
    {ddl, DDL, _} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 15, 's')), a,b,c))")),
    ?assertEqual(
        "field_orders() -> [ascending,ascending,ascending].",
        build_field_orders_fn_string(DDL )
    ).

build_field_orders_fn_string_asc_desc_test() ->
    {ddl, DDL, _} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 15, 's')), a ASC,b DESC,c ASC))")),
    ?assertEqual(
        "field_orders() -> [ascending,descending,ascending].",
        build_field_orders_fn_string(DDL)
    ).

build_revert_ordering_on_local_key_fn_string_test() ->
    {ddl, DDL, _} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
        "CREATE TABLE temperatures ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 15, 's')), a ASC,b DESC,c ASC))")),
    ?assertEqual(
        "revert_ordering_on_local_key({A,B,C}) -> [A,riak_ql_ddl:flip_binary(B),C].",
        build_revert_ordering_on_local_key_fn_string(DDL)
    ).

build_extract_fn_source_test() ->
    {ddl, DDL, _} = riak_ql_parser:ql_parse(riak_ql_lexer:get_tokens(
        "CREATE TABLE mytab ("
        "a VARCHAR NOT NULL, "
        "b VARCHAR NOT NULL, "
        "c TIMESTAMP NOT NULL, "
        "PRIMARY KEY ((a,b,quantum(c, 15, 's')), a ASC,b DESC,c ASC))")),
    ?assertEqual(
        "extract(Obj, [<<\"a\">>]) when is_tuple(Obj) -> element(1, Obj);\n"
        "extract(Obj, [<<\"b\">>]) when is_tuple(Obj) -> element(2, Obj);\n"
        "extract(Obj, [<<\"c\">>]) when is_tuple(Obj) -> element(3, Obj).",
        build_extract_fn_source(DDL)
    ).


-endif.
