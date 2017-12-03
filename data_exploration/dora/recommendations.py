from .datasources import SqlSource

class Recommendations(SqlSource):
    def insert(self, customerid, productid, rank, timestamp):
        self._execSqlInsert('''
            INSERT INTO recommendations
            (
               customerid,
               productid,
               "rank",
               "timestamp"
            )
            VALUES
            (
                %(customerid)s,
                %(productid)s,
                %(rank)s,
                %(timestamp)s
            )''',
            {
                'customerid':customerid,
                'productid':productid,
                'rank':rank,
                'timestamp':timestamp
            })