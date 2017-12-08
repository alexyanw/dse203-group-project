from hybrid_engine import HybridEngine
import logging

# single table
datalog = [{
    'result': 'Ans(productid, fullprice)',
    'table': ['postgres.products(productid, _, _, _, _, fullprice, _, _)'],
    'condition': ['fullprice > 100']
    }]

# negation
datalog1 = [{
    'result': 'Ans(customerid, orderid, numunits, totalprice)',
    'table': ['postgres.orders(orderid, customerid, _, _, _, _, _, _, totalprice, _, _)',
            'postgres.customers(customerid, _, gender, firstname)',
            'not postgres.customers(customerid, _, gender, "John")',
            ],
    }]

# single source with original column name
# 3 way joining and condition, orderby
datalog2 = [{
    'result': 'Ans(customerid, productid, numunits, billdate)',
    'table': ['postgres.orders(oid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, numUnits)',
            'postgres.customers(customerid, householdId, gender, firstname)',
            'postgres.orderlines(orderLineId, oid, productId, shipDate, billdate, unitPrice, numunits, totalPrice)'],
    'condition': ['oid > 1000', 'numunits > 1'],
    'orderby': 'numunits DESC',
    }]

# predefined view:
datalog3 = [{
    'result': 'ans(pid, spring, summer, fall, winter)',
    'table': ['postgres.seasonal_percentages(pid, spring, summer, fall, winter)'],
    'limit': '10'
    }]

# predefined views: product_view
datalog4 = [{
    'result': 'Ans(productid, total_review_count)',
    'table': ['postgres.product_view(productid, nodeid, fullprice, isinstock, total_review_count, avg_review_rating , total_order_count , total_copy_count )']
    }]

# predefined views: product_orders
datalog5 = [{
    'result': 'Ans(productid, total_order_count)',
    'table': ['postgres.product_orders(productid, total_order_count, total_copy_count)']
    }]

# predefined views: coocurrence_matrix
datalog6 = [{
    'result': 'Ans(product_a, product_b, pair_count)',
    'table': ['postgres.cooccurrence_matrix(product_a, product_b, pair_count)']
    }]

# distinct - group without aggregation
datalog7 = [
        {
            'result': 'Ans(asin)',
            'table': ['postgres.orders(orderid, customerid, _, _, _, _, _, _, _, _, _)',
            'postgres.customers(customerid, _, _, _)',
            'postgres.orderlines(_, orderid, productid, _, _, _, _, _)',
            'postgres.products(productid,_,_,_,_,_,asin,_)'
                     ],
            'condition': ['customerid=68099'],
            'groupby': {'key':'asin'}
        }
    ]

# group with aggregation
#q(X, A) :- setof({Z}, {Y }, p(X, Y, Z), S), count(S, A)
datalog8 = [
    {
    'result': 'Ans(pid, total_orders, total_value)',
    'table': ['postgres.orders(oid, _, _, _, _, _, _, _, _, _, _)',
            'postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
           ],
    'condition': ['pid > 1000', "date > '2015-01-01'"],
    'groupby': { 'key': 'pid', 'aggregation': ['count(oid, total_orders)', 'sum(price, total_value)']},
    'limit': '10'
    }
]

# join of predefined views
datalog9 = [{
    'result': 'Ans(product_a, product_b, pair_count)',
    'table': ['postgres.cooccurrence_matrix(product_a, product_b, pair_count)',
            'postgres.product_view(product_a, _, fullprice, _, _, _ , total_order_count , total_copy_count)'],
    'condition': ['total_order_count > 10', 'fullprice < 100']
    }]

# view, aggregation
datalog10 = [{
    'result': 'CoMat(product_a, product_b, pair_count)',
    'table': ['postgres.cooccurrence_matrix(product_a, product_b, pair_count)',
            'postgres.product_view(product_a, _, fullprice, isinstock, _, _ , total_order_count , total_copy_count)',
            'not postgres.product_view(product_a, _, _, "N", _, _ , _ , _)'
            ],
    'condition': ['total_order_count > 1', 'fullprice < 100'],
    },
    {
        'result': 'Ans(product_a, total_co_count)',
        'table': ['view.CoMat(product_a, product_b, pair_count)'],
        'groupby': {'key': 'product_a', 'aggregation': ['sum(pair_count, total_co_count)']}
        #'q(product_a, A) :- setof({pair_count}, E^b. CoMat(product_a, b, pair_count), S), sum(S, A)'
    }]

# union
datalog11 = [{
    'result': 'cust_prod_view(cid, pid, nunits)',
    'table': ['postgres.orders(oid, cid, _, _, _, _, _, _, _, _, _)',
	'postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
	'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
	'postgres.customers(cid, _, _, _)'],
    },
    {
	'result': 'ans(cid, pid, nunits)',
	'table': ['view.cust_prod_view(cid, pid, nunits)'],
	'condition': ['nunits > 1']
    }]

engine = HybridEngine(
                postgres= {'server': 'localhost', 'port': 5432, 'database': 'SQLBook', 'user': 'postgres', 'password': 'pavan007'},
                asterix= {'server': 'localhost', 'port': 19002, 'dataverse': 'TinySocial'},
                solr= {'server': 'localhost', 'port': 8983, 'core': 'bookstore'})

#result = engine.queryDatalog(datalog, debug=True, loglevel=logging.DEBUG)
#result = engine.queryDatalog(datalog, loglevel=logging.DEBUG)


# view chain
datalog = '''
view1(olid, oid, nunits) <-
  postgres.products(pid, _, _, _, _, _, asin, nodeid),
  postgres.orderlines(olid, oid, pid, _, date, _, nunits, price),
  date > '2015-01-01'
view2(state, cid, nunits) <-
  view.view1(olid, oid, nunits),
  postgres.orders(oid, cid, _, _, _, state, _, _, _, _, _),
view3(state, gender, nunits) <-
  view.view2(state, cid, nunits),
  postgres.customers(cid, _, gender, _),
Ans(state, gender, total_units) <-
  setof({nunits}, view3(state, gender, nunits), S), sum(nunits, total_units)
'''

result = engine.queryDatalogRaw(datalog, loglevel=logging.INFO)
print(result)
