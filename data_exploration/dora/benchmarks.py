from .datasources import SqlSource

class Benchmarks(SqlSource):
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