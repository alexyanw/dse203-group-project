# wrapper class of product features from postgresql
import sys
from functools import reduce

class ProductView:
    schema = {
        'product_view': ['productid', 'nodeid', 'fullprice', 'isinstock', 'total_review_count', 'avg_review_rating', 'total_order_count', 'total_copy_count'],
        'product_self': ['productid', 'nodeid', 'fullprice', 'isinstock'],
        'product_reviews' : ['productid', 'total_review_count', 'avg_review_rating'],
        'product_orders'  : ['productid', 'total_order_count', 'total_copy_count'],
    }

    def __init__(self):
        self.schema_map = {
            'product_self': (self.get_product, ['productid', 'nodeid', 'fullprice', 'isinstock']),
            'product_reviews' : (self.get_product_reviews, ['productid', 'total_review_count', 'avg_review_rating']),
            'product_orders'  : (self.get_product_orders, ['productid', 'total_order_count', 'total_copy_count']),
            #'total_copy_count'   : ('product_orders', self.get_product_orders),
        }

    @classmethod
    def getColumn(cls, table, idx):
        if table not in cls.schema:
            print("Required view '{}' doesn't exist\n".format(table))
            exit(1)
        return cls.schema[table][idx]

    def _execute(self, viewname, cmd, **kwargs):
        dbcmd = kwargs.get('prefix', '')
        if 'view' in kwargs:
            dbcmd += "{} as ({})".format(viewname, cmd)
            return dbcmd
        dbcmd += cmd
        if 'limit' in kwargs:
            dbcmd += "LIMIT {}".format(kwargs['limit'])
        return dbcmd


        #return self.dbengine.execute(dbcmd)

    def get_product(self, **kwargs):
        cmd = '''
SELECT p.productid, p.nodeid, p.fullprice, p.isinstock
FROM products p
'''
        return self._execute('product_self', cmd, **kwargs)

    def get_product_orders(self, **kwargs):
        cmd = '''
SELECT p.productid, count(ol) as total_order_count, SUM(ol.numunits) as total_copy_count
FROM products p, orderlines ol
WHERE p.productid = ol.productid
GROUP BY p.productid
'''
        return self._execute('product_orders', cmd, **kwargs)

    def get_product_reviews(self, **kwargs):
        cmd = '''
SELECT p.productid, avg(r.overall) as avg_review_rating, COUNT(1) as total_review_count
FROM products p, reviews r
WHERE p.asin = r.asin
GROUP BY p.productid
'''
        return self._execute('product_reviews', cmd, **kwargs)

    def get_product_view(self, features=[], **kwargs):
        valid_features = set(reduce(lambda a, b: a+b, [v[1] for v in self.schema_map.values()], []))
        if len(features) == 0:
            features = valid_features
        else:
            # validate features against schema_map
            invalid_features = set(features) - valid_features
            if invalid_features:
                print('get_product_view not support features:', invalid_features)
                return None
            features = set(features)

        subviews = self.schema_map.keys()
        feature_map = {}
        for feature in features:
            for view,schema in self.schema_map.items():
                if feature in schema[1]:
                    feature_map[feature] = view
                    break

        func_ptrs = [self.schema_map[feature_map[f]][0] for f in feature_map]
        exist = {}
        view_contents = []
        for f in func_ptrs:
            if f in exist: continue
            view_contents.append(f(view=True))
            exist[f] = True
        join_keys = ["{}.productid = product_self.productid".format(sv) for sv in (set(subviews) - set(['product_self']))]
        feature_list = [feature_map[f]+'.'+f for f in features]
        dbcmd = '''
SELECT {}
FROM {}
WHERE {}
'''.format(', '.join(feature_list), ', '.join(subviews), ' AND '.join(join_keys))
        
        if 'where' in kwargs:
            dbcmd += "AND {}\n".format(kwargs['where'])

        view_contents.append(self._execute('product_view', dbcmd, **kwargs))
        return view_contents

    # uniform interface
    def get_views(self, features=[], **kwargs):
        return self.get_product_view(features, **kwargs)

        
