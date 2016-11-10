-module(riak_ql_pfitting_arithmetic).
-behaviour(riak_ql_pfitting).
-behaviour(gen_server).
-export([start_link/0]).
-export([init/1,
         terminate/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3]).

-export([define_column_mappings/2,
         get_output_columns/1,
         process/3]).

-export([create/1,
         create_arithmetic_column_mapping/3]).

-record(state, {
          column_mappings :: [riak_ql_pfitting:column_mapping()]
         }).

%% TODO: include SQL_NULL instead
-ifndef(SQL_NULL).
-define(SQL_NULL, []).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. %%TEST

-spec create_arithmetic_column_mapping(atom(),
                                       riak_ql_pfitting:constant_or_identifier(),
                                       riak_ql_pfitting:constant_or_identifier()) ->
    riak_ql_pfitting:column_mapping().
create_arithmetic_column_mapping(ArithmeticFunA, LHS, RHS) ->
    DisplayText = column_as_arithmetic(ArithmeticFunA, LHS, RHS),
    OutputType = unresolved,
    riak_ql_pfitting:create_column_mapping(DisplayText, DisplayText,
                                           OutputType,
                                           arithmetic_fun_atom_to_fun(ArithmeticFunA),
                                           [LHS, RHS]).

-spec create([riak_ql_pfitting:column_mapping()]) -> {ok, pid()}.
create(ColumnMappings) ->
    Res = {ok, Pid} = start_link(),
    define_column_mappings(Pid, ColumnMappings),
    Res.

arithmetic_fun_atom_to_fun(add) ->
    fun arithmetic_fun_add/2;
arithmetic_fun_atom_to_fun(subtract) ->
    fun arithmetic_fun_subtract/2;
arithmetic_fun_atom_to_fun(multiply) ->
    fun arithmetic_fun_multiply/2;
arithmetic_fun_atom_to_fun(divide) ->
    fun arithmetic_fun_divide/2.

constant_as_binary(C) ->
    [C1] = io_lib:format("~p", [C]),
    list_to_binary(C1).

column_as_arithmetic(add, LHS, RHS) ->
    column_as_arithmetic(<<" + ">>, LHS, RHS);
column_as_arithmetic(subtract, LHS, RHS) ->
    column_as_arithmetic(<<" - ">>, LHS, RHS);
column_as_arithmetic(multiply, LHS, RHS) ->
    column_as_arithmetic(<<" * ">>, LHS, RHS);
column_as_arithmetic(divide, LHS, RHS) ->
    column_as_arithmetic(<<" / ">>, LHS, RHS);
column_as_arithmetic(AFunL, {identifier, IdentifierL}, {identifier, IdentifierR}) ->
    <<IdentifierL/binary, AFunL/binary, IdentifierR/binary>>;
column_as_arithmetic(AFunL, {identifier, IdentifierL}, {constant, ConstantR}) ->
    CR = constant_as_binary(ConstantR),
    <<IdentifierL/binary, AFunL/binary, CR/binary>>;
column_as_arithmetic(AFunL, {constant, ConstantL}, {identifier, IdentifierR}) ->
    CL = constant_as_binary(ConstantL),
    <<CL/binary, AFunL/binary, IdentifierR/binary>>;
column_as_arithmetic(AFunL, {constant, ConstantL}, {constant, ConstantR}) ->
    CR = constant_as_binary(ConstantR),
    CL = constant_as_binary(ConstantL),
    <<CL/binary, AFunL/binary, CR/binary>>.

%% riak_ql_pfitting
define_column_mappings(Pid, ColumnMappings) ->
    gen_server:call(Pid, {define_column_mappings, ColumnMappings}).

get_output_columns(Pid) ->
    gen_server:call(Pid, {get_output_columns}).

process(Pid, Columns, Rows) ->
    gen_server:call(Pid, {process, Columns, Rows}).

%% gen_server
start_link() ->
    gen_server:start_link(?MODULE, [], []).

init(_Args) ->
    {ok, #state{}}.

terminate(normal, _State) ->
    ok.

handle_call({define_column_mappings, ColumnMappings}, _From, State) ->
    {reply, ok, State#state{column_mappings = ColumnMappings}};
handle_call({get_output_columns}, _From,
            State=#state{column_mappings = undefined}) ->
    {reply, {ok, []}, State};
handle_call({get_output_columns}, _From,
            State=#state{column_mappings = ColumnMappings}) ->
    OutputColumnNames =
        [ riak_ql_pfitting:get_column_mapping_display_text(ColumnMapping) ||
          ColumnMapping <- ColumnMappings ],
    {reply, {ok, OutputColumnNames}, State};
handle_call({process, Columns, Rows}, _From,
            State = #state{column_mappings = ColumnMappings }) ->
    Res = process_arithmetic(Columns, Rows, ColumnMappings),
    ResStatus = riak_ql_pfitting:get_process_result_status(Res),
    {reply, {ResStatus, Res}, State};
handle_call(_Req, _From, State) ->
    {noreply, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Req, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

arithmetic_fun(_F, ?SQL_NULL, ?SQL_NULL) ->
    ?SQL_NULL;
arithmetic_fun(_F, _LHS, ?SQL_NULL) ->
    ?SQL_NULL;
arithmetic_fun(_F, ?SQL_NULL, _RHS) ->
    ?SQL_NULL;
arithmetic_fun(F, LHS, RHS) ->
    F(LHS, RHS).

add(LHS, RHS) -> LHS + RHS.
arithmetic_fun_add(LHS, RHS) -> arithmetic_fun(fun add/2, LHS, RHS).

subtract(LHS, RHS) -> LHS - RHS.
arithmetic_fun_subtract(LHS, RHS) -> arithmetic_fun(fun subtract/2, LHS, RHS).

multiply(LHS, RHS) -> LHS * RHS.
arithmetic_fun_multiply(LHS, RHS) -> arithmetic_fun(fun multiply/2, LHS, RHS).

divide(LHS, RHS) -> LHS / RHS.
arithmetic_fun_divide(LHS, RHS) -> arithmetic_fun(fun divide/2, LHS, RHS).

deref_value({constant, C}, _Row, _Columns) ->
    C;
deref_value({identifier, _I}, [], []) ->
    ?SQL_NULL;
deref_value({identifier, I}, [RH|_RT], [CH|_CT]) when CH =:= I ->
    RH;
deref_value(V={identifier, _I}, [_RH|RT], [_CH|CT]) ->
    deref_value(V, RT, CT).

map_row(Row, Columns, ColumnMappings) ->
    map_row_(Row, Columns, ColumnMappings, []).
map_row_(_Row, _Columns, [], Agg) ->
    lists:reverse(Agg);
map_row_(Row, Columns, [{ArithmeticFun, [LHS, RHS]}|ColumnMappingT], Agg) ->
    LHSV = deref_value(LHS, Row,Columns),
    RHSV = deref_value(RHS, Row, Columns),
    V = arithmetic_fun(ArithmeticFun, LHSV, RHSV),
    map_row_(Row, Columns, ColumnMappingT, [V|Agg]).

process_arithmetic(Columns, Rows, ColumnMappings) ->
    ArithmeticColumns = [ riak_ql_pfitting:get_column_mapping_display_text(ColumnMapping) ||
                     ColumnMapping <- ColumnMappings ],
    ColumnMappings1 = [ {riak_ql_pfitting:get_column_mapping_fun(ColumnMapping),
                         riak_ql_pfitting:get_column_mapping_fun_args(ColumnMapping)}||
                                 ColumnMapping <- ColumnMappings],
    { ProcessRows, ProcessErrors} =
    try [ map_row(Row, Columns, ColumnMappings1) || Row <- Rows] of
        V -> {V, []}
    catch
        error:Error -> {[], [Error]}
    end,
    riak_ql_pfitting:create_process_result(ArithmeticColumns,
                                           ProcessRows, ProcessErrors).

-ifdef(TEST).
column_as_arithmetic_identifiers_test() ->
    ?assertEqual(<<"ab + cd">>,
                 column_as_arithmetic(add,
                                       {identifier, <<"ab">>},
                                       {identifier, <<"cd">>})).

column_as_arithmetic_identifier_constant_test() ->
    ?assertEqual(<<"ab / 17">>,
                 column_as_arithmetic(divide,
                                       {identifier, <<"ab">>},
                                       {constant, 17})).

column_as_arithmetic_constant_identifier_test() ->
    ?assertEqual(<<"13.0 * cd">>,
                 column_as_arithmetic(multiply,
                                       {constant, 13.0},
                                       {identifier, <<"cd">>})).

column_as_arithmetic_constants_test() ->
    ?assertEqual(<<"1000 - 27.0">>,
                 column_as_arithmetic(subtract,
                                       {constant, 1000},
                                       {constant, 27.0})).

map_simple_arithmetic_column_mappings(SimpleColumnMappings) ->
    [create_arithmetic_column_mapping(ArithmeticFunA, LHS, RHS) ||
     {ArithmeticFunA, LHS, RHS} <- SimpleColumnMappings].

process_setup(SimpleColumnMappings) ->
    ColumnMappings = map_simple_arithmetic_column_mappings(SimpleColumnMappings),
    {ok, Pid} = ?MODULE:create(ColumnMappings),
    Columns = [<<"r">>, <<"i">>, <<"a">>, <<"k">>],
    Rows = [[1, <<"one">>, 1000, 1.0],
            [[],[],[],[]],
            [[], <<"two">>, 2000, 2.0],
            [3, [], 3000, 3.0],
            [4, <<"four">>, [], 4.0],
            [5, <<"five">>, 5000, []]],
    {Pid, Columns, Rows}.

expected_rows_single_identifier(ArithmeticFun, FieldExtractorFun, Constant, Rows) ->
    [[ArithmeticFun(FieldExtractorFun(Row), Constant)] ||
     Row <- Rows].

expected_rows_two_identifiers(ArithmeticFun, FieldExtractorFun, Rows) ->
    AFun = fun([LHS, RHS]) -> ArithmeticFun(LHS, RHS) end,
    [[AFun(FieldExtractorFun(Row))] ||
     Row <- Rows].

process_add_third_field_and_constant_test() ->
    ArithmeticFunA = add,
    ArithmeticFun = arithmetic_fun_atom_to_fun(ArithmeticFunA),
    LHS = {identifier, <<"a">>},
    Constant = 50,
    RHS = {constant, Constant},
    ExpectedColumns = [ column_as_arithmetic(ArithmeticFunA, LHS, RHS) ],
    {Pid, Columns, Rows} = process_setup([{ArithmeticFunA, LHS, RHS}]),
    ExpectedRows = expected_rows_single_identifier(ArithmeticFun,
                                                   fun ([_F1, _F2, F3, _F4]) -> F3 end,
                                                   Constant, Rows),
    ExpectedErrors = [],
    Processed = ?MODULE:process(Pid, Columns, Rows),
    ?assertEqual({ok, riak_ql_pfitting:create_process_result(ExpectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_subtract_constant_and_constant_test() ->
    ArithmeticFunA = subtract,
    ConstantL = 73,
    LHS = {constant, ConstantL},
    ConstantR = 51,
    RHS = {constant, ConstantR},
    ExpectedColumns = [ column_as_arithmetic(ArithmeticFunA, LHS, RHS) ],
    {Pid, Columns, Rows} = process_setup([{ArithmeticFunA, LHS, RHS}]),
    ExpectedRows = [[ConstantL - ConstantR] || _Row <- Rows],
    ExpectedErrors = [],
    Processed = ?MODULE:process(Pid, Columns, Rows),
    ?assertEqual({ok, riak_ql_pfitting:create_process_result(ExpectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_divide_two_fields_test() ->
    ArithmeticFunA = divide,
    ArithmeticFun = arithmetic_fun_atom_to_fun(ArithmeticFunA),
    LHS = {identifier, <<"a">>},
    RHS = {identifier, <<"k">>},
    ExpectedColumns = [ column_as_arithmetic(ArithmeticFunA, LHS, RHS) ],
    {Pid, Columns, Rows} = process_setup([{ArithmeticFunA, LHS, RHS}]),
    ExpectedRows = expected_rows_two_identifiers(ArithmeticFun,
                                                   fun ([_F1, _F2, F3, F4]) -> [F3, F4] end,
                                                   Rows),
    ExpectedErrors = [],
    Processed = ?MODULE:process(Pid, Columns, Rows),
    ?assertEqual({ok, riak_ql_pfitting:create_process_result(ExpectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_multiply_badarith_test() ->
    ArithmeticFunA = multiply,
    LHS = {constant, 3},
    RHS = {identifier, <<"i">>},
    ExpectedColumns = [ column_as_arithmetic(ArithmeticFunA, LHS, RHS) ],
    {Pid, Columns, Rows} = process_setup([{ArithmeticFunA, LHS, RHS}]),
    ExpectedRows = [],
    ExpectedErrors = [badarith],
    Processed = ?MODULE:process(Pid, Columns, Rows),
    ?assertEqual({error, riak_ql_pfitting:create_process_result(ExpectedColumns,
                                                                ExpectedRows,
                                                                ExpectedErrors)},
                 Processed).

-endif. %%TEST
