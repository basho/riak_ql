%% -------------------------------------------------------------------
%%
%% riak_ql_pspout - pspout emitting a result set to be piped through pfittings.
%%
%% Copyright (c) 2016-2017 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_ql_pspout).

-record(pspout, {
          module :: module(),
          open :: function()
         }).
-opaque pspout() :: #pspout{}.
-export_type([pspout/0]).

-export([open/1,
         create/2]).

-callback create(proplists:proplist()) -> pspout().

-spec create(module(), function()) -> pspout().
create(Module, OpenFn) ->
    #pspout{module = Module, open = OpenFn}.

-spec open(pspout()) -> riak_ql_pf_result:process_result().
open(#pspout{module = _Module, open = OpenFn}) ->
    OpenFn().
