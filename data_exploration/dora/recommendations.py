from .datasources import SqlSource


class Recommendations(SqlSource):
    def statsByProduct(self, productids=[], min_date='1900-1-1', max_date=None, sample_size=100):
        product_filter = ' AND productid IN %(product_list)s ' if len(productids) > 0 else ' '
        max_date_filter = ' AND timestamp <= %(max_date)s' if max_date else ' '

        return self._execSqlQuery('''
            SELECT
                productid,
                ROUND(AVG(rank),2) avg_rank,
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

    def rankDistribution(self, min_date='1900-1-1', max_date=None, productid=[], sample_size=100):
        """For each product, determine the how many recommendation ranks (1->10) the product received.

        Args:
            min_date (string): optional. date. inclusive bottom limit of reviewTime
            max_date (string): optional. date. inclusive upper limit of reviewTime
            productid (list): optional. The asins of the products the rating distrubtion will be produced for. Defaults to returning distributions for all asins
            sample_size (int): optional. Percentage of the reviews the query will run over.

        Returns:
           QueryResponse:
               columns (:obj:`list` of :obj:`str`): [asin, productid, 'rank_one', 'rank_two', 'rank_three','rank_four', 'rank_five', 'rank_six', 'rank_seven', 'rank_eight', 'rank_nine', 'rank_ten]

               results (:obj:`list` of :obj:`tuple(str,int,int,int,int,int,int)`)
        """

        max_date_filter = ' AND timestamp <= %(max_date)s' if max_date else ' '

        asin_filter = ' AND productid IN %(product_list)s ' if len(productid) > 0 else ' '

        query = ('''
                  SELECT
                    productid,
                    SUM(
                        CASE
                          WHEN rank = 1
                          THEN 1
                          ELSE 0
                        END) as rank_one,
                    SUM(
                        CASE
                          WHEN rank = 2
                          THEN 1
                          ELSE 0
                        END) as rank_two,
                    SUM(
                        CASE
                          WHEN rank = 3
                          THEN 1
                          ELSE 0
                        END) as rank_three,
                    SUM(
                        CASE
                          WHEN rank = 4
                          THEN 1
                          ELSE 0
                        END) as rank_four,
                    SUM(
                        CASE
                          WHEN rank = 5
                          THEN 1
                          ELSE 0
                        END) as rank_five
                  FROM recommendations
                  TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s)

                  WHERE
                    timestamp >= %(min_date)s'''
                 + max_date_filter
                 + asin_filter
                 + '''
                  GROUP BY productid
                  ORDER BY rank_one DESC''')

        return self._execSqlQuery(query,
                                  {
                                      'min_date': min_date,
                                      'max_date': max_date,
                                      'product_list': tuple(productid),
                                      'sample_size': sample_size,
                                      'random_seed': self._random_seed
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
