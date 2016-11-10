-module(riak_ql_pfitting_project).
-behaviour(riak_ql_pfitting).
-behaviour(gen_server).

-record(state, {
          column_mappings :: [riak_ql_pfitting:column_mapping()]
         }).

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
         create_projection_column_mapping/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif. %%TEST

-spec create_projection_column_mapping(binary()) -> riak_ql_pfitting:column_mapping().
create_projection_column_mapping(ColumnIdentifier) ->
    riak_ql_pfitting:create_column_mapping(ColumnIdentifier,
                                           ColumnIdentifier,
                                           unresolved).

-spec create([riak_ql_pfitting:column_mapping()]) -> {ok, pid()}.
create(ColumnMappings) ->
    Res = {ok, Pid} = start_link(),
    define_column_mappings(Pid, ColumnMappings),
    Res.

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
    Res = process_projection(Columns, Rows, ColumnMappings),
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

project_row(Row, Columns, ColumnMappingIdentifiers) ->
    project_row_(Row, Columns, ColumnMappingIdentifiers, Row, Columns, []).
project_row_(Row, Columns, Columns, _Row, _Columns, []) ->
    Row;
project_row_([], _Columns, _ColumnMappingIdentifiers, _ORow, _OColumns, Acc) ->
    lists:reverse(Acc);
project_row_(_Row, _Columns, [], _ORow, _OColumns, Acc) ->
    lists:reverse(Acc);
project_row_([RowV|_RowT], [Column|_ColumnT], [Column|ColumnMappingIdentifiersT], ORow, OColumns, Acc) ->
    project_row_(ORow, OColumns, ColumnMappingIdentifiersT, ORow, OColumns, [RowV|Acc]);
project_row_([_RowV|RowT], [_Column|ColumnT], ColumnMappingIdentifiers, ORow, OColumns, Acc) ->
    project_row_(RowT, ColumnT, ColumnMappingIdentifiers, ORow, OColumns, Acc).

assert_column_mappings(Columns, ColumnMappings) ->
    assert_column_mappings_(Columns, ColumnMappings, Columns).
assert_column_mappings_(_Columns, [], _Columns) ->
    pass;
assert_column_mappings_([], [ColumnMappingH|_ColumnMappingT], _Columns) ->
    %% throw({invalid_column_mapping, ColumnMappingH});
    throw({invalid_column_mapping, ColumnMappingH});
assert_column_mappings_([ColumnH|_ColumnT], [ColumnH|ColumnMappingT], Columns) ->
    assert_column_mappings_(Columns, ColumnMappingT, Columns);
assert_column_mappings_([_ColumnH|ColumnT], ColumnMappings, Columns) ->
    assert_column_mappings_(ColumnT, ColumnMappings, Columns).

process_projection(Columns, Rows, ColumnMappings) ->
    ProcessColumns = [riak_ql_pfitting:get_column_mapping_input_identifier(ColumnMapping) ||
                         ColumnMapping <- ColumnMappings],
    {ProcessRows, ProcessErrors} = try
                         assert_column_mappings(Columns, ProcessColumns),
                         {[project_row(Row, Columns, ProcessColumns) ||
                          Row <- Rows],
                          []}
                     catch
                         throw:{invalid_column_mapping, ColumnMapping} -> {[], [{invalid_column_mapping, ColumnMapping}]}
                     end,
    riak_ql_pfitting:create_process_result(ProcessColumns,
                                           ProcessRows, ProcessErrors).

-ifdef(TEST).

process_setup(ColumnIdentifiers) ->
    ColumnMappings = [create_projection_column_mapping(ColumnIdentifier) ||
                      ColumnIdentifier <- ColumnIdentifiers],
    {ok, Pid} = ?MODULE:create(ColumnMappings),
    Columns = [<<"r">>, <<"i">>, <<"a">>, <<"k">>],
    Rows = [[1, <<"one">>, 1000, 1.0],
            [[], <<"two">>, 2000, 2.0],
            [3, [], 3000, 3.0],
            [4, <<"four">>, [], 4.0],
            [5, <<"five">>, 5000, []]],
    {Pid, Columns, Rows}.

process_projection_empty_test() ->
    ProjectedColumns = [],
    {Pid, Columns, Rows} = process_setup(ProjectedColumns),
    ExpectedRows = [[] || _Row <- Rows],
    ExpectedErrors = [],
    Processed = ?MODULE:process(Pid, Columns, Rows),
    ?assertEqual({ok, riak_ql_pfitting:create_process_result(ProjectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_projection_same_test() ->
    ProjectedColumns = [<<"r">>,<<"i">>,<<"a">>,<<"k">>],
    {Pid, Columns, Rows} = process_setup(ProjectedColumns),
    ExpectedRows = Rows,
    ExpectedErrors = [],
    Processed = ?MODULE:process(Pid, Columns, Rows),
    ?assertEqual({ok, riak_ql_pfitting:create_process_result(ProjectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_projection_rearranged_test() ->
    ProjectedColumns = [<<"i">>,<<"r">>,<<"k">>,<<"a">>],
    {Pid, Columns, Rows} = process_setup(ProjectedColumns),
    ExpectedRows = [[V1,V0,V3,V2] ||
                    [V0,V1,V2,V3] <- Rows],
    ExpectedErrors = [],
    Processed = ?MODULE:process(Pid, Columns, Rows),
    ?assertEqual({ok, riak_ql_pfitting:create_process_result(ProjectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_projection_reduced_test() ->
    ProjectedColumns = [<<"r">>,<<"a">>,<<"i">>],
    {Pid, Columns, Rows} = process_setup(ProjectedColumns),
    ExpectedRows = [[V0,V2,V1] ||
                    [V0,V1,V2,_V3] <- Rows],
    ExpectedErrors = [],
    Processed = ?MODULE:process(Pid, Columns, Rows),
    ?assertEqual({ok, riak_ql_pfitting:create_process_result(ProjectedColumns,
                                                             ExpectedRows,
                                                             ExpectedErrors)},
                 Processed).

process_projection_invalid_identifier_test() ->
    ProjectedColumns = [<<"r">>,<<"a">>,<<"i">>,<<"n">>],
    {Pid, Columns, Rows} = process_setup(ProjectedColumns),
    ExpectedRows = [],
    ExpectedErrors = [{invalid_column_mapping, <<"n">>}],
    Processed = ?MODULE:process(Pid, Columns, Rows),
    ?assertEqual({error, riak_ql_pfitting:create_process_result(ProjectedColumns,
                                                                ExpectedRows,
                                                                ExpectedErrors)},
                 Processed).

-endif. %%TEST
