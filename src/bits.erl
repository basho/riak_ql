-module(bits).

-export([
         log_terms/1
        ]).

log_terms(Terms) ->
    io:put_chars(standard_error, Terms).
