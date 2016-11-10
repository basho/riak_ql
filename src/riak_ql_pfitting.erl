-module(riak_ql_pfitting).

-type constant_or_identifier() :: {constant, term()} |
                                  {identifier, binary()}.

-type column_mapping_function() :: function().
-type column_mapping_function_args() :: [constant_or_identifier()].

-type resolvable_field_type() :: riak_ql_ddl:simple_field_type() | unresolved.
-record(column_mapping, {
          input_identifier :: binary(),
          output_display_text :: binary(),
          output_type :: resolvable_field_type(),
          mapping_fun :: column_mapping_function(),
          mapping_fun_args :: column_mapping_function_args()
         }).
-opaque column_mapping() :: #column_mapping{}.
-export_type([column_mapping/0]).

-type columns() :: [binary()].
-type rows() :: [[term()]].
-type errors() :: [atom() | {atom(), term()}].
-record(process_result, {
          columns :: columns(),
          rows :: rows(),
          errors :: errors()
         }).
-opaque process_result() :: #process_result{}.
-export_type([process_result/0]).

-export([create_column_mapping/3,
         create_column_mapping/4,
         create_column_mapping/5,
         get_column_mapping_input_identifier/1,
         get_column_mapping_output_type/1,
         get_column_mapping_display_text/1,
         get_column_mapping_fun/1,
         get_column_mapping_fun_args/1,
         nop_mapping/2,
         null_mapping/2,
         create_process_result/3,
         get_process_result_columns/1,
         get_process_result_rows/1,
         get_process_result_errors/1,
         get_process_result_status/1]).

-callback define_column_mappings(pid(), [column_mapping()]) -> term().
-callback get_output_columns(pid()) -> [binary()].
-callback process(pid(), [binary()], [[term()]]) -> process_result().

-spec create_column_mapping(binary(), binary(), resolvable_field_type()) -> column_mapping().
create_column_mapping(InputIdentifier, OutputDisplayText, OutputType) ->
    create_column_mapping(InputIdentifier, OutputDisplayText, OutputType, fun nop_mapping/2).

-spec create_column_mapping(binary(), binary(), resolvable_field_type(),
                            column_mapping_function()) -> column_mapping().
create_column_mapping(InputIdentifier, OutputDisplayText, OutputType, MappingFun) ->
    create_column_mapping(InputIdentifier, OutputDisplayText, OutputType, MappingFun, []).

-spec create_column_mapping(binary(), binary(), resolvable_field_type(),
                            column_mapping_function(),
                            column_mapping_function_args()) -> column_mapping().
create_column_mapping(InputIdentifier, OutputDisplayText, OutputType, MappingFun, MappingFunArgs) ->
    #column_mapping{input_identifier = InputIdentifier,
                    output_display_text = OutputDisplayText,
                    output_type = OutputType,
                    mapping_fun = MappingFun,
                    mapping_fun_args = MappingFunArgs
                   }.

-spec get_column_mapping_input_identifier(column_mapping()) -> binary().
get_column_mapping_input_identifier(#column_mapping{input_identifier = InputIdentifier}) ->
    InputIdentifier.

-spec get_column_mapping_output_type(column_mapping()) -> resolvable_field_type().
get_column_mapping_output_type(#column_mapping{output_type = OutputType}) ->
    OutputType.

-spec get_column_mapping_display_text(column_mapping()) -> binary().
get_column_mapping_display_text(#column_mapping{output_display_text=OutputDisplayText}) ->
    OutputDisplayText.

-spec get_column_mapping_fun(column_mapping()) -> column_mapping_function().
get_column_mapping_fun(#column_mapping{mapping_fun = MappingFun}) ->
    MappingFun.

-spec get_column_mapping_fun_args(column_mapping()) -> [term()].
get_column_mapping_fun_args(#column_mapping{mapping_fun_args = MappingFunArgs}) ->
    MappingFunArgs.

-spec nop_mapping(term(), term()) -> term().
nop_mapping(In, _Out) ->
    In.

-spec null_mapping(term(), term()) -> term().
null_mapping(_In, _Out) ->
    undefined.

-spec create_process_result(columns(), rows(), errors()) -> process_result().
create_process_result(Columns, Rows, Errors) ->
    #process_result{columns = Columns,
                    rows = Rows,
                    errors = Errors}.

-spec get_process_result_columns(process_result()) -> columns().
get_process_result_columns(#process_result{columns = Columns}) -> Columns.

-spec get_process_result_rows(process_result()) -> rows().
get_process_result_rows(#process_result{rows = Rows}) -> Rows.

-spec get_process_result_errors(process_result()) -> errors().
get_process_result_errors(#process_result{errors = Errors}) -> Errors.

-spec get_process_result_status(process_result()) -> ok|error.
get_process_result_status(#process_result{errors=[]}) -> ok;
get_process_result_status(_) -> error.
