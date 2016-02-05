%% -------------------------------------------------------------------
%%
%% An escript command for linting riak_ql queries. Create the
%% riak_ql command by running:
%%
%%    ./rebar escriptize
%%
%% There should now be a runnable riak_ql file in the project
%% directory. Usage:
%%
%%     ./riak_ql "SELECT * FROM my_table"
%%     ./riak_ql "CREATE TABLE my_table(my_field int, PRIMARY KEY(my_field))"
%%
%% This lints the last given argument, a syntax error is printed
%% if one exists, otherwise there is no output. To print out the
%% generated ddl add -ddl before the last command:
%%
%%     ./riak_ql -ddl "SELECT * FROM my_table"
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

-module(riak_ql_cmd).

-export([main/1]).

-include("riak_ql_ddl.hrl").

%%
main([_|_] = Args) ->
    Query = lists:last(Args),
    Lexed = riak_ql_lexer:get_tokens(Query),
    case riak_ql_parser:parse(Lexed) of
        {ok, DDL} ->
            maybe_print_ddl(Args, DDL);
        {error, {Token,_,_}} ->
            io:format("Error: syntax error before ~s~n", [Token]),
            % return an error code for the proc if an error has occurred
            erlang:halt(1)
    end;
main([]) ->
    io:format(
        "Invalid usage, try: ./riak_ql \"SELECT * FROM my_table\"~n").

%%
maybe_print_ddl(Args, DDL) ->
    case lists:member("-ddl", Args) of
        true  -> io:format("~p~n", [DDL]);
        false -> ok
    end.
