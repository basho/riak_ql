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
	  'FROM'        = <<>>  :: binary() | {list, [binary()]} | {regex, list()},
	  'WHERE'       = []    :: [filter()],
	  'ORDER BY'    = []    :: [sorter()],
	  'LIMIT'       = []    :: [limit()],
	  'INNER JOIN'  = [],
	  'ON'          = [],
	  helper_mod            :: atom(),
	  partition_key = none  :: none | binary(),
	  is_executable = false :: boolean(),
	  type          = sql   :: sql | timeseries,
	  local_key                                  % prolly a mistake to put this here - should be in DDL
	}).

-record(riak_sql_insert_v1,
	{
	  'INSERT INTO' = [],
	  'VALUES'      = []
	}).
