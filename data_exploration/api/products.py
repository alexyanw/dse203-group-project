from datasources import SqlSource
from logger import log
from datetime import datetime

class Products(SqlSource):
    @log
    def ratingsDistribution(self, min_date='1900-1-1', max_date=None, sample_size=100, asin=[]):

        max_date_filter =  ' AND r.ReviewTime <= %(max_date)s' if max_date else ' '

        asin_filter = ' AND r.asin IN %(asin_list)s ' if len(asin) > 0 else ' '

        query = ('''
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
                r.ReviewTime >= %(min_date)s'''
                + max_date_filter
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
                'asin_list':tuple(asin),
                'sample_size':sample_size,
                'random_seed':self._random_seed
            })

    @log
    def coPurchases(self, asin, min_date='1900-1-1', max_date=None, sample_size=100 ):
        if len(asin) == 0: 
            return None
        
        max_date_filter = ' AND o.orderdate <= %(max_date)s ' if max_date else ' '

        query = ('''
            SELECT
              p.asin,
              count(ol.productid) as numPurch
            FROM
              products p,
              orderlines ol
              TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s),
              orders o
            WHERE
              o.orderid = ol.orderid
              AND o.orderdate > %(min_date)s'''+max_date_filter+'''
              AND ol.orderid in (
                SELECT orid.orderid
                FROM (
                  SELECT
                    orderlines.orderid,
                    orderlines.productid
                  FROM
                    orderlines,
                    (
                      SELECT products.productid
                      FROM products
                      WHERE products.asin in %(asin_list)s
                    ) as pid
                  WHERE orderlines.productid=pid.productid
                ) as orid
                WHERE
                  orid.orderid in (
                    SELECT orderlines.orderid
                    FROM orderlines
                    GROUP BY orderlines.orderid
                    having count(orderlines.orderid) >1
                  )
              )
              AND ol.productid=p.productid
              AND p.asin not in %(asin_list)s
            GROUP BY p.asin
            ORDER BY numPurch DESC;''')
        return self._execSqlQuery (query,
               {
                    'min_date':min_date,
                    'max_date':max_date,
                    'asin_list':tuple(asin),
                    'sample_size':sample_size,
                    'random_seed':self._random_seed
               })
        
    @log
    def clusterQuery(self):
        query = ( '''
                SELECT orderlines.productid,
                    products.asin,
                    count(*) as num_orders,
                    avg(regexp_replace(orderlines.totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as totalprice_avg,
                    r.overall,
                    products.category,
                    case 
                        when calendar.month=3 and calendar.dom>=21 then 1
                        when calendar.month=4 then 1
                        when calendar.month=5 then 1
                        when calendar.month=6 and calendar.dom<21 then 1
                        when calendar.month=6 and calendar.dom>=21 then 2
                        when calendar.month=7 then 2
                        when calendar.month=8 then 2
                        when calendar.month=9 and calendar.dom<23 then 2
                        when calendar.month=9 and calendar.dom>=23 then 3
                        when calendar.month=10 then 3
                        when calendar.month=11 then 3
                        when calendar.month=12 and calendar.dom<21 then 3
                        when calendar.month=12 and calendar.dom>=21 then 4
                        when calendar.month=1 then 4
                        when calendar.month=2 then 4
                        when calendar.month=3 and calendar.dom<21 then 4
                        else 0
                    end as season

                FROM orderlines
                    JOIN products
                      ON orderlines.productid = products.productid
                    JOIN orders o
                      ON orderlines.orderid = o.orderid
                    JOIN reviews r 
                      ON products.asin=r.asin
                    JOIN calendar  
                      ON o.orderdate=calendar.date
                WHERE orderlines.numunits > 0
                group by orderlines.productid, products.asin, r.overall, products.category, season''')
        return self._execSqlQuery(query)


                                          
        
        