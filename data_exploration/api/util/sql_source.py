import psycopg2

class SqlQueryResponse:
    def __init__(self, columns, results):
        self.columns = columns
        self.results = results

class SqlSource:
    def __init__(self, connection_string, random_seed=1):
        self._postgres_conn = psycopg2.connect(connection_string)
        self._random_seed = random_seed

    def __del__(self):
        self._postgres_conn.close()

    def _execSqlQuery(self,query, params=None):
        c = self._postgres_conn.cursor()

        c.execute(query,params)
        results = c.fetchall()
        columns = [desc[0] for desc in c.description]

        c.close()
        return SqlQueryResponse(
            columns=columns,
            results=results
        )