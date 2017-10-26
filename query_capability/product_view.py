# wrapper class of product features from postgresql
import pandas as pd
import sys
from sqlalchemy import create_engine
import numpy as np

# features
#price (fullprice in products table)
#isinstock( products table)
#avg review rating (products & reviews on asin)
#total_review_count
#review_helpful_rate
#total_sold_copies_current_month
# dynamic features
#total_sold_copies_during
#total_sold_copies_channel
class ProductView:
    def __init__(self, server = 'localhost', port = 5432, database = 'SQLBook'):
        dburl = 'postgresql://postgres:@' + server + ':' + str(port) + '/' + database
        self.pg_conn = create_engine(dburl)
        self.feature_map = {
            'fullprice' : self.get_product_orders,
            'isinstock' : self.get_product_orders,
            'total_review_count' : self.get_product_reviews,
            'avg_review_rating'  : self.get_product_reviews,
            #'review_helpful_rate': self.get_product_reviews,
            'total_order_count'  : self.get_product_orders,
            'total_copy_count'   : self.get_product_orders,
        }

    def _execute(self, cmd, **kwargs):
        if kwargs['limit']:
            cmd += "LIMIT {}".format(kwargs['limit'])
        print(cmd)
        df = pd.read_sql_query(cmd, self.pg_conn)
        return df

    def get_product_orders(self, **kwargs):
        cmd = '''
SELECT p.productid, p.fullprice, p.isinstock, count(ol) as total_order_count, SUM(ol.numunits) as total_copy_count
FROM products p, orderlines ol
WHERE p.productid = ol.productid
GROUP BY p.productid
'''
        if kwargs['view']:
            cmd = "product_orders as (\n{}\n)".format(cmd)
            return cmd

        return self._execute(cmd, **kwargs)

    def get_product_reviews(self, **kwargs):
        cmd = '''
SELECT p.productid, avg(r.overall) as avg_review_rating, COUNT(1) as total_review_count
FROM products p, reviews r
WHERE p.asin = r.asin
GROUP BY p.productid
'''
        if kwargs['view']:
            cmd = "product_reviews as (\n{}\n)".format(cmd)
            return cmd
        return self._execute(cmd, **kwargs)

    def get_product_view(self, features=[], **kwargs):
        if len(features) == 0:
            features = self.feature_map.keys()
        else:
            # validate features against feature_map
            invalid_features = set(features) - set(self.feature_map.keys())
            if invalid_features:
                print('get_product_view not support features:', invalid_features)
                return None

        func_ptrs = set(self.feature_map[f] for f in features)
        cmd = 'WITH '
        cmd += ",\n".join([f(view=True) for f in func_ptrs])
        # HACK: hardcode view names
        cmd += '''
SELECT {}
FROM product_orders po, product_reviews pr
WHERE po.productid = pr.productid
'''.format(', '.join(features))
        return self._execute(cmd, **kwargs)


        
