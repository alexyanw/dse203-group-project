from .datasources import SqlSource


class Recommendations(SqlSource):
    def statsByProduct(self, productids=[], min_date='1900-1-1', max_date=None, sample_size=100):
        product_filter = ' AND productid IN %(product_list)s ' if len(productids) > 0 else ' '
        max_date_filter = ' AND timestamp <= %(max_date)s' if max_date else ' '

        return self._execSqlQuery('''
            SELECT
                productid,
                AVG(rank) avg_rank,
                COUNT(*) as times_recommended
            FROM recommendations
            TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s)
            WHERE
                timestamp >= %(min_date)s'''
                + max_date_filter
                + product_filter + '''
            GROUP BY productid
            ORDER BY times_recommended DESC''',
            {
                'product_list':productids,
                'min_date':min_date,
                'max_date':max_date,
                'random_seed':self._random_seed,
                'sample_size':sample_size
            })

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
                                'customerid': customerid,
                                'productid': productid,
                                'rank': rank,
                                'timestamp': timestamp
                            })
