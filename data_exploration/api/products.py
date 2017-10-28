from util.sql_source import SqlSource
from datetime import datetime

class Products(SqlSource):
    def ratingsDistribution(self, min_date='1900-1-1', max_date=None, sample_size=100, asin=[]):
        max_date = datetime.now() if max_date==None else max_date

        asin_filter = (' AND r.asin IN %(asin_list)s ' if len(asin) > 1
            else ' AND r.asin = %(asin_list)s ' if len(asin) == 1
            else ' ')

        query = ('''
            CREATE EXTENSION IF NOT EXISTS "tsm_system_rows";
            WITH CTE as (
              SELECT
                p.asin,
                p.productid,
                r.reviewtime,
                r.overall
              FROM products p
              LEFT JOIN reviews r
                TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s)
                ON p.asin = r.asin
              WHERE
                r.ReviewTime >= %(min_date)s
                AND r.ReviewTime <= %(max_date)s'''
                + asin_filter
                + '''ORDER BY r.overall
            )  SELECT
                asin,
                productid,
                SUM(
                    CASE
                      WHEN overall = 1
                      THEN 1
                      ELSE 0
                    END) as one_star_votes,
                SUM(
                    CASE
                      WHEN overall = 2
                      THEN 1
                      ELSE 0
                    END) as two_star_votes,
                SUM(
                    CASE
                      WHEN overall = 3
                      THEN 1
                      ELSE 0
                    END) as three_star_votes,
                SUM(
                    CASE
                      WHEN overall = 4
                      THEN 1
                      ELSE 0
                    END) as four_star_votes,
                SUM(
                    CASE
                      WHEN overall = 5
                      THEN 1
                      ELSE 0
                    END) as five_star_votes
              FROM CTE
              GROUP BY asin, productid
              ORDER BY five_star_votes DESC''')

        return self._execSqlQuery(query,
            {
                'min_date':min_date,
                'max_date':max_date,
                'asin_list':asin,
                'sample_size':sample_size,
                'random_seed':self._random_seed
            })