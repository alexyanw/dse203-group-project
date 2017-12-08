from .datasources import SqlSource

class Benchmarks(SqlSource):

    def statsByFunction(self, min_date='1900-1-1', max_date=None, function_filter=[], sample_size=100):
        """Execution stats for API functions.

        Stats aggregated by function_name and is_cached.

        Args:
            min_date (string): optional. date. Limits the search result timeframe .
            max_date (string): optional. date. Limits the search result timeframe.
            function_filter (None or list): optional. List of function names to query.
            sample_size (int): optional. Percentage of the benchmarks the query will run over.

        Returns:
            QueryResponse:
                columns (:obj:`list` of :obj:`str`): ['function_name','is_cached','avg_runtime_seconds','total_runtime_seconds','invocations']

                results (:obj:`list` of :obj:`tuple(str,bool,float,float,int))`
        """
        function_filter = ' AND function_name IN %(function_list)s ' if (type(function_filter) is list) & (len(function_filter) > 0) else ' '
        max_date_filter = ' AND start <= %(max_date)s' if max_date else ' '

        return self._execSqlQuery('''
            SELECT
                b.function_name,
                b.is_cached,
                AVG(EXTRACT(EPOCH from age(b.end,b.start))) avg_runtime_seconds,
                SUM(EXTRACT(EPOCH from age(b.end,b.start))) total_runtime_seconds,
                count(*) as invocations
            FROM eda_benchmarks b
            TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s)
            WHERE
                start >= %(min_date)s'''
                + max_date_filter
                + function_filter + '''
            GROUP BY b.function_name, b.is_cached
            ORDER BY avg_runtime_seconds desc
        ''',{
                'function_list':function_filter,
                'min_date':min_date,
                'max_date':max_date,
                'random_seed':self._random_seed,
                'sample_size':sample_size
            })

    def clientActivity(self, aggregate=False, min_date='1900-1-1', max_date=None, clientid_filter=[], sample_size=100):
        """Hourly aggregate API activity by client.

        Args:
            aggregate (bool): individual results or aggregate all clients
            min_date (string): optional. date. Limits the search result timeframe .
            max_date (string): optional. date. Limits the search result timeframe.
            clientid_filter (None or list): optional. List of clientids to query.
            sample_size (int): optional. Percentage of the benchmarks the query will run over.

        Returns:
            QueryResponse:
                columns (:obj:`list` of :obj:`str`): ['clientid','hour','api_calls']

                results (:obj:`list` of :obj:`tuple(str,int,int))`
        """

        max_date_filter = ' AND start <= %(max_date)s ' if max_date else ' '

        if not aggregate:
            client_filter = ' AND c.clientid IN %(client_list)s ' if (type(clientid_filter) is list) & (len(clientid_filter) > 0) else ' '
            return self._execSqlQuery('''
                WITH
                    hours AS (
                        SELECT generate_series(0,23) as ofDay
                    ),
                    clients AS (
                        SELECT DISTINCT clientid
                        FROM eda_benchmarks
                    )
                SELECT
                    c.clientid,
                    h.ofDay as hour,
                    count(b.*) AS api_calls
                FROM
                    clients c
                CROSS JOIN hours h
                LEFT JOIN eda_benchmarks b
                TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s)
                    ON
                        c.clientid = b.clientid
                        AND h.ofDay = DATE_PART('hour', b.start)
                WHERE
                    b.benchmarkid IS NULL
                    OR (
                        start >= %(min_date)s'''
                        + max_date_filter
                        + client_filter + ''')
                GROUP BY c.clientid, h.ofDay
                ORDER BY c.clientid, h.ofDay;
            ''',{
                'client_list':tuple(clientid_filter),
                'min_date':min_date,
                'max_date':max_date,
                'random_seed':self._random_seed,
                'sample_size':sample_size
            })
        else:
            return self._execSqlQuery('''
                WITH
                    hours AS (
                        SELECT generate_series(0,23) as ofDay
                    ),
                    clients AS (
                        SELECT DISTINCT clientid
                        FROM eda_benchmarks
                    )
                SELECT
                    h.ofDay as hour,
                    count(b.*) AS api_calls
                FROM
                    clients c
                CROSS JOIN hours h
                LEFT JOIN eda_benchmarks b
                TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s)
                    ON
                        c.clientid = b.clientid
                        AND h.ofDay = DATE_PART('hour', b.start)
                WHERE
                    b.benchmarkid IS NULL
                    OR (
                        start >= %(min_date)s'''
                        + max_date_filter +
                        ''')
                GROUP BY h.ofDay
                ORDER BY h.ofDay;
            ''',{
                'client_list':clientid_filter,
                'min_date':min_date,
                'max_date':max_date,
                'random_seed':self._random_seed,
                'sample_size':sample_size
            })

    def insert(self, function, args, kwargs, start, end, is_cached, client_id):
        """Log an API call to SqlSource."""
        self._execSqlInsert('''
            INSERT INTO eda_benchmarks
            (
                function_name,
                args,
                kwargs,
                is_cached,
                "end",
                start,
                clientid
            )
            VALUES
            (
                %(function)s,
                %(args)s,
                %(kwargs)s,
                %(is_cached)s,
                %(end)s,
                %(start)s,
                %(client_id)s
            )''',
            {
                'function':function,
                'args':args,
                'kwargs':kwargs,
                'is_cached':is_cached,
                'end':end,
                'start':start,
                'client_id':client_id
            })