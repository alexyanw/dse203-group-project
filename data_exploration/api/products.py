from util.sql_source import SqlSource
from datetime import datetime

class Products(SqlSource):
    def ratingsDistribution(self, min_date='1900-1-1', max_date=None, sample_size=1):
        max_date = datetime.now() if max_date==None else max_date

        return self._execSqlQuery('''
            SELECT
              p.asin,
              p.productid,
              coalesce(one_star_votes,0) as one_star_votes,
              coalesce(two_star_votes,0) as two_star_votes,
              coalesce(three_star_votes,0) as three_star_votes,
              coalesce(four_star_votes,0) as four_star_votes,
              coalesce(five_star_votes,0) as five_star_votes
            FROM products p
            LEFT JOIN (
                SELECT
                  asin,
                  count(*) as one_star_votes
                FROM reviews
                WHERE
                    overall = 1
                    AND ReviewTime >= %(min_date)s
                    AND ReviewTime <= %(max_date)s
                GROUP BY asin) r1
              on
                p.asin = r1.asin
            LEFT JOIN (
                SELECT
                  asin,
                  count(*) as two_star_votes
                FROM reviews
                WHERE
                    overall = 2
                    AND ReviewTime >= %(min_date)s
                    AND ReviewTime <= %(max_date)s
                GROUP BY asin) r2
              on
                p.asin = r2.asin
            LEFT JOIN (
                SELECT
                  asin,
                  count(*) as three_star_votes
                FROM reviews
                WHERE
                    overall = 3
                    AND ReviewTime >= %(min_date)s
                    AND ReviewTime <= %(max_date)s
                GROUP BY asin) r3
              on
                p.asin = r3.asin
            LEFT JOIN (
                SELECT
                  asin,
                  count(*) as four_star_votes
                FROM reviews
                WHERE
                    overall = 4
                    AND ReviewTime >= %(min_date)s
                    AND ReviewTime <= %(max_date)s
                GROUP BY asin) r4
              on
                p.asin = r4.asin
            LEFT JOIN (
                SELECT
                  asin,
                  count(*) as five_star_votes
                FROM reviews
                WHERE
                    overall = 5
                    AND ReviewTime >= %(min_date)s
                    AND ReviewTime <= %(max_date)s
                GROUP BY asin) r5
              on
                p.asin = r5.asin;''', {'min_date':min_date,'max_date':max_date})