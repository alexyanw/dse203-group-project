# wrapper class of product features from postgresql
import sys
from functools import reduce

# CoOccurrenceMatrix(productA, productB, total_count, zipcode, gender, start_date, end_date)
class CoOccurrenceMatrix:
    def __init__(self):
        self.schema_map = {
            'customer_product' : (self.get_customer_product, ['customerid', 'productid', 'likes', 'gender', 'orderdate','orderstate']),
            'cooccurrence_matrix' : (self.get_cooccurrence_matrix, ['product_a', 'product_b', 'pair_count']),
        }

    def _execute(self, viewname, cmd, **kwargs):
        dbcmd = kwargs.get('prefix', '')
        if 'view' in kwargs:
            dbcmd += "{} as ({})".format(viewname, cmd)
            return dbcmd
        dbcmd += cmd
        if 'limit' in kwargs:
            dbcmd += "LIMIT {}".format(kwargs['limit'])
        return dbcmd

    def get_customer_product(self, **kwargs):
        cmd = '''
SELECT c.customerid, pr.productid, 1 as likes, c.gender as gender, o.orderdate, o.state as orderstate, o.zipcode as zipcode
FROM customers c, orders o, orderlines ol, products pr
WHERE o.customerid = c.customerid and ol.orderid = o.orderid and pr.productid = ol.productid
'''
        if 'customer_product_condition' in kwargs:
            cmd += ' AND ' + kwargs['customer_product_condition']
        return self._execute('customer_product', cmd, **kwargs)

    def get_cooccurrence_matrix(self, **kwargs):
        cmd = '''
SELECT cp1.productid as product_a, cp2.productid as product_b, count(cp1.customerid) as paircount
FROM cust_prod cp1, cust_prod cp2
WHERE cp1.customerid = cp2.customerid AND cp1.productid < cp2.productid
GROUP BY cp1.productid, cp2.productid
ORDER BY COUNT(cp1.customerid)  DESC
'''
        return self._execute('cooccurrence_matrix', cmd, **kwargs)

    def get_views(self, features=[], **kwargs):
        views = []
        table = kwargs.get('table', None)
        views.append(self.get_customer_product(**kwargs))
        if table in [None, 'cooccurrence_matrix']:
            views.append(self.get_cooccurrence_matrix(**kwargs))
        return views
	
