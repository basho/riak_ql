#!/usr/bin/env ruby

require 'csv'
require 'pp'

ERLANG_RESERVED = %w{and of or}
DISAMBIGUATED = %W{float int}

csv_filename =
  ARGV[0] ||
  File.join(File.dirname(__FILE__), '..', 'src', 'riak_ql_keywords.csv')

keywords = Hash.new

CSV.foreach(csv_filename) do |row|
  keyword = row.first
  kw_name = keyword.gsub(/\s+/,'_').upcase
  keywords[kw_name] = keyword
end

# definitions
# SELECT  = (S|s)(E|e)(L|l)(E|e)(C|c)(T|t)
keywords.each do |kw_name, keyword|
  kw_regexp =
    keyword.chars.map do |c|
    next '\s' if c =~ /\s/
    next c unless c =~ /[a-zA-Z]/
    "(#{c.upcase}|#{c.downcase})"
  end.join

  puts "#{kw_name} = #{kw_regexp}"
end

puts

# rules
# {SELECT}  : {token, {select,  TokenChars}}.
keywords.each do |kw_name, keyword|
  kw_token = kw_name.downcase
  if ERLANG_RESERVED.include? kw_token
    kw_token += '_'
  end
  puts "{#{kw_name}} : {token, {#{kw_token}, TokenChars}}."
end

puts

# these two sets generate the when guard for the 
# token post processor that concatenates the token

# char concatenator (1)
# this generates the post-processor clauses in the lexer
# needs to start with chars
puts "Word1 =:= chars orelse"
keywords.each do |kw_name, keyword|
  kw_token = kw_name.downcase
  if ERLANG_RESERVED.include? kw_token
    kw_token += '_'
  end
  if DISAMBIGUATED.include? kw_token
    kw_token += '_type'
  end
  puts "Word1 =:= #{kw_token} orelse"
end

puts
puts "%% add the rest of the where clause here"
puts "%% a comma should seperate these clauses"
puts
# char concatenator (2)
# this generates the post-processor clauses in the lexer
# needs to start with chars
puts "Word2 =:= chars orelse"
keywords.each do |kw_name, keyword|
  kw_token = kw_name.downcase
  if ERLANG_RESERVED.include? kw_token
    kw_token += '_'
  end
  if DISAMBIGUATED.include? kw_token
    kw_token += '_type'
  end
  puts "Word2 =:= #{kw_token} orelse"
end

# also need to add the int type to the second half the guard clause
puts "Word2 =:= int ->"
