#!/usr/bin/env ruby

require 'csv'
require 'pp'

ERLANG_RESERVED = %w{and of or}

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
