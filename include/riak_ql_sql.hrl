%% TODO these types will be improved over the duration of the time series project
-type selection()  :: term().
-type filter()     :: term().
-type operator()   :: term().
-type sorter()     :: term().
-type combinator() :: term().
-type limit()      :: any().

-record(riak_sql_v1,
	{
	  'SELECT'      = []    :: [selection() | operator() | combinator()],
	  'FROM'        = <<>>  :: binary(),     % TODO fix up
	  'WHERE'       = []    :: [filter()],
	  'ORDER BY'    = []    :: [sorter()],
	  'LIMIT'       = []    :: [limit()],
	  partition_key = none  :: none | binary(),
	  is_executable = false :: boolean()
	}).
