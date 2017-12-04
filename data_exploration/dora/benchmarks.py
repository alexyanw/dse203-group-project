from .datasources import SqlSource

class Benchmarks(SqlSource):

    def statsByFunction(self):
        return self._execSqlQuery('''
            SELECT
                b.function_name,
                b.is_cached,
                AVG(EXTRACT(EPOCH from age(b.end,b.start))) avg_runtime_seconds,
                SUM(EXTRACT(EPOCH from age(b.end,b.start))) total_runtime_seconds,
                count(*) as invocations
            FROM eda_benchmarks b
            GROUP BY b.function_name, b.is_cached
            ORDER BY avg_runtime_seconds desc
        ''')

    def clientActivity(self, aggregate=False):
        if not aggregate:
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
                    ON
                        c.clientid = b.clientid
                        AND h.ofDay = DATE_PART('hour', b.start)
                GROUP BY c.clientid, h.ofDay
                ORDER BY c.clientid, h.ofDay;
            ''')
        else:
            return self._execSqlQuery('''
                WITH
                    hours AS (
                        SELECT generate_series(0,23) as ofDay
                    )
                SELECT
                    h.ofDay as hour,
                    count(b.*) AS api_calls
                FROM hours h
                LEFT JOIN eda_benchmarks b
                    ON
                     h.ofDay = DATE_PART('hour', b.start)
                GROUP BY h.ofDay
                ORDER BY h.ofDay;
            ''')

    def insert(self, function, args, kwargs, start, end, is_cached, client_id):
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