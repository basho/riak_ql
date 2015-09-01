%% TODO these types will be improved over the duration of the time series project
-type selection()  :: [binary()].
-type filter()     :: term().
-type operator()   :: [binary()].
-type sorter()     :: term().
-type combinator() :: [binary()].
-type limit()      :: any().

-record(riak_sql_v1,
	{
	  'SELECT'      = []    :: [selection() | operator() | combinator()],
	  'FROM'        = <<>>  :: binary() | {list, [binary()]} | {regex, list()},
	  'WHERE'       = []    :: [filter()],
	  'ORDER BY'    = []    :: [sorter()],
	  'LIMIT'       = []    :: [limit()],
	  helper_mod            :: atom(),
	  partition_key = none  :: none | binary(),
	  is_executable = false :: boolean(),
	  type          = sql   :: sql | timeseries,
	  local_key                                  % prolly a mistake to put this here - should be in DDL
	}).
