# Introduction

This document outlines how to add new keywords to the lexer:

1. Add them to the `priv/riak_ql_keywords.csv` file, one per line. Keeping this file in alphabetic order simplifies future changes.
2. Run `ruby priv/keyword_generator.rb` with Ruby 1.9.3 or newer.
3. Replace the keyword definitions near the top of `src/riak_ql_lexer.xrl` with the regex definitions (first chunk of output) from the message generator.
4. Replace the keyword rules in the `src/riak_ql_lexer.xrl` file with the second chunk of output from the message generator.
5. Save and commit the csv and lexer changes.

If this is done correctly, the commit diff should simply be a few lines added to the csv and a few lines added to the lexer.
