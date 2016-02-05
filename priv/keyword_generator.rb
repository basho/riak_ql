#!/usr/bin/env ruby
# -------------------------------------------------------------------
#
# Generate the Erlang keyword definition from the CSV file
#
# Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------------

require 'csv'
require 'pp'

ERLANG_RESERVED = %w{and of or not}

csv_filename =
  ARGV[0] ||
  File.join(File.dirname(__FILE__), '.', 'riak_ql_keywords.csv')

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
  puts "{#{kw_name}} : {token, {#{kw_token}, list_to_binary(TokenChars)}}."
end
