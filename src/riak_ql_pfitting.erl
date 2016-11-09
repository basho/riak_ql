-module(riak_ql_pfitting).

-type constant_or_identifier() :: {constant, term()} |
                                  {identifier, binary()}.

-record(column_mapping, {
          input_identifier :: binary(),
          output_display_text :: binary(),
          output_type :: riak_ql_ddl:simple_field_type(),
          mapping_fun :: function(),
          mapping_fun_args :: [constant_or_identifier()]
         }).
-opaque column_mapping() :: #column_mapping{}.
-export_type([column_mapping/0]).

-export([behaviour_info/1]).
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
         get_process_status/1]).

behaviour_info(callbacks) ->
    [{define_column_mappings, 2},
     {get_output_columns, 1},
     {process, 3}];
behaviour_info(_) ->
    undefined.

create_column_mapping(InputIdentifier, OutputDisplayText, OutputType) ->
    create_column_mapping(InputIdentifier, OutputDisplayText, OutputType, fun nop_mapping/2).
create_column_mapping(InputIdentifier, OutputDisplayText, OutputType, MappingFun) ->
    create_column_mapping(InputIdentifier, OutputDisplayText, OutputType, MappingFun, []).
create_column_mapping(InputIdentifier, OutputDisplayText, OutputType, MappingFun, MappingFunArgs) ->
    #column_mapping{input_identifier = InputIdentifier,
                    output_display_text = OutputDisplayText,
                    output_type = OutputType,
                    mapping_fun = MappingFun,
                    mapping_fun_args = MappingFunArgs
                   }.

get_column_mapping_input_identifier(#column_mapping{input_identifier = InputIdentifier}) ->
    InputIdentifier.
get_column_mapping_output_type(#column_mapping{output_type = OutputType}) ->
    OutputType.
get_column_mapping_display_text(#column_mapping{output_display_text=OutputDisplayText}) ->
    OutputDisplayText.
get_column_mapping_fun(#column_mapping{mapping_fun = MappingFun}) ->
    MappingFun.
get_column_mapping_fun_args(#column_mapping{mapping_fun_args = MappingFunArgs}) ->
    MappingFunArgs.

nop_mapping(In, _Out) ->
    In.

null_mapping(_In, _Out) ->
    undefined.

get_process_status([_Rows, _Columns, _Errors=[]]) -> ok;
get_process_status([_Rows, _Columns, _Errors]) -> error.
