%% -------------------------------------------------------------------
%%
%% riak_ql_pfitting_column_mapping: Riak Query Pipeline pipe fitting (pfitting)
%% column mapping datatype used in creating pfittings.
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
-module(riak_ql_pfitting_column_mapping).

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

-export([create/3,
         create/5,
         get_input_identifier/1,
         get_output_type/1,
         get_display_text/1,
         get_fun/1,
         get_fun_args/1,
         nop_mapping/2,
         null_mapping/2]).

-spec create(binary(), binary(), resolvable_field_type()) -> column_mapping().
create(InputIdentifier, OutputDisplayText, OutputType) ->
    create(InputIdentifier, OutputDisplayText, OutputType, fun nop_mapping/2, []).

-spec create(binary(), binary(), resolvable_field_type(),
             column_mapping_function(), column_mapping_function_args()) -> column_mapping().
create(InputIdentifier, OutputDisplayText, OutputType, MappingFun, MappingFunArgs) ->
    #column_mapping{input_identifier = InputIdentifier,
                    output_display_text = OutputDisplayText,
                    output_type = OutputType,
                    mapping_fun = MappingFun,
                    mapping_fun_args = MappingFunArgs
                   }.

-spec get_input_identifier(column_mapping()) -> binary().
get_input_identifier(#column_mapping{input_identifier = InputIdentifier}) -> InputIdentifier.

-spec get_output_type(column_mapping()) -> resolvable_field_type().
get_output_type(#column_mapping{output_type = OutputType}) -> OutputType.

-spec get_display_text(column_mapping()) -> binary().
get_display_text(#column_mapping{output_display_text=OutputDisplayText}) -> OutputDisplayText.

-spec get_fun(column_mapping()) -> column_mapping_function().
get_fun(#column_mapping{mapping_fun = MappingFun}) -> MappingFun.

-spec get_fun_args(column_mapping()) -> [term()].
get_fun_args(#column_mapping{mapping_fun_args = MappingFunArgs}) -> MappingFunArgs.

-spec nop_mapping(term(), term()) -> term().
nop_mapping(In, _Out) -> In.

-spec null_mapping(term(), term()) -> term().
null_mapping(_In, _Out) -> undefined.
