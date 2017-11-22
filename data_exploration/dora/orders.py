from .datasources import SqlSource

class Orders(SqlSource):

    def statsByZipcode(self, min_date='1900-1-1', max_date=None, sample_size=100):
        max_date_filter = ' AND o.orderdate <= %(max_date)s' if max_date else ' '

        return self._execSqlQuery('''
            SELECT
                z.countyname,
                z.countypop,
                count(o.orderid) as NumofOrders,
                sum(o.totalprice) as TotalSpending
            FROM
                zipcounty z,
                orders o
                TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s)
            WHERE
                z.zipcode = o.zipcode
                AND o.orderdate >= %(min_date)s'''
                + max_date_filter + '''
            GROUP BY
                z.countyname,
                z.countypop''',
            {
               'sample_size':sample_size,
                'random_seed':self._random_seed,
                'min_date':min_date,
                'max_date':max_date
            })

    def statsByProduct(self, min_date='1900-1-1', max_date=None, sample_size=100, order_by='num_orders'):
        max_date_filter = ' AND o.orderdate <= %(max_date)s' if max_date else ' '

        return self._execSqlQuery('''
            SELECT
                orderlines.productid,
                products.asin,
                count(*) as num_orders,
                min(shipdate) as first_order,
                max(shipdate) as last_order,
                DATE_PART('day', max(shipdate)::TIMESTAMP - min(shipdate)::TIMESTAMP) as days_on_sale,
                min(regexp_replace(unitprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as unitprice_min,
                max(regexp_replace(unitprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as unitprice_max,
                avg(regexp_replace(unitprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as unitprice_avg,
                min(orderlines.numunits) as numunits_min,
                max(orderlines.numunits) as numunits_max,
                avg(orderlines.numunits) as numunits_avg,
                sum(orderlines.numunits) as numunits_sum,
                min(regexp_replace(orderlines.totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as totalprice_min,
                max(regexp_replace(orderlines.totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as totalprice_max,
                avg(regexp_replace(orderlines.totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as totalprice_avg,
                sum(regexp_replace(orderlines.totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as totalprice_sum
            FROM orderlines
            TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s)
            JOIN products
              ON orderlines.productid = products.productid
            JOIN orders o
              ON orderlines.orderid = o.orderid
            WHERE
                orderlines.numunits > 0
                 AND o.orderdate >= %(min_date)s'''
                + max_date_filter + '''
            GROUP BY orderlines.productid, products.asin
            ORDER BY {} DESC'''.format(order_by),
            {
              'sample_size': sample_size,
              'random_seed': self._random_seed,
              'min_date': min_date,
              'max_date': max_date
            })