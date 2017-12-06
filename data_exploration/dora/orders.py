from .datasources import SqlSource
from .logger import log
from datetime import datetime

class Orders(SqlSource):

    @log
    def statsByZipcode(self, min_date='1900-1-1', max_date=None, sample_size=100):
        """For each zipcode, determine the number of orders and the total amount of money has been spent. 

        Args:
            min_date (string): optional. date. Limits the search result timeframe.
            max_date (string): optional. date. Limits the search result timeframe.
            sample_size (int): optional. Percentage of the data the query will run over.
        
        Returns:
            QueryResponse:
                columns (:obj:`list` of :obj:`str`): ['countyname', 'countypop', 'NumofOrders', 'TotalSpending']

                results (:obj:`list` of :obj:`tuple(str,int,int,float))`

                countyname is the name of the
                county the zipcode corresonds to. countypop is the population of the county. NumofOrders is
                the number of orders that have been purchased by customers in the zipcode. TotalSpending is
                the amount of money customers in the zipcode have purchased."""
        
        max_date_filter = ' AND o.orderdate <= %(max_date)s' if max_date else ' '

        query=('''
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
                z.countypop''')
        
        return self._execSqlQuery(query, 
            {
               'sample_size':sample_size,
                'random_seed':self._random_seed,
                'min_date':min_date,
                'max_date':max_date
            })

    def statsByProduct(self, min_date='1900-1-1', max_date=None, sample_size=100):
        """Produces statistics for each product.  

        Args:
            min_date (string): optional. date. Limits the search result timeframe.
            max_date (string): optional. date.Limits the search result timeframe.
            sample_size (int): optional. Percentage of the orders the query will run over.
        
        Returns:
            QueryResponse:
                columns (:obj:`list` of :obj:`str`): ['productid', 'asin', 'num_orders',
                    'first_order', 'last_order', 'days_on_sale', 'unitprice_min','unitprice_max',
                    'uniteprice_avg', 'numunits_min', 'numunits_max', 'numunits_avg', 'numunites_sum',
                    'totalprice_min', 'totalprice_max', 'totalprice_avg', 'totalprice_sum']

                results (:obj:`list` of :obj:`tuple(str,int,int,float))`

                productid is the unique identifier for the product. asin is the asin for the product.
                first_order is the date of the first product being shipped.
                last_order is the last day an order was shipped. days_on_sale
                is the total number of days the product was on sale. unitprice_min is the minimum price the
                product. unitprice_max is the maximum price for the product. unitprice_avg is the average
                price for the product. numunits_min is the minimum number of times the book was purchased in
                one order. numunits_max is the largest number of times the book was purchased in one order.
                numunits_avg is the average number of times the book was purchased in the same order.
                numunits_sum is the number of times the book was purchased."""

        max_date_filter = ' AND o.orderdate <= %(max_date)s' if max_date else ' '

        query=('''
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
            ORDER BY count(*) DESC''')
        return self._execSqlQuery(query,
              {                        
              'sample_size': sample_size,
              'random_seed': self._random_seed,
              'min_date': min_date,
              'max_date': max_date
            })