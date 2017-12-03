from hybrid_engine import HybridEngine

# single source with original column name
datalog1 = [
    {
    'result': 'Ans(numunits, firstname, billdate, orderid, customerid)',
    'table': ['postgres.orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, _)',
            'postgres.customers(customerid, householdId, gender, firstname)',
            'postgres.orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, _)',
           ],
    'condition': ['orderid > 1000', 'numunits > 1'],
    'orderby': 'numunits DESC',
    'limit': '10'
    }
]

# single source with column renaming
datalog2 = [
    {
    'result': 'Ans(nunits, fname, date, oid, customerid)',
    'table': ['postgres.orders(oid, customerid, _, _, _, _, _, _, _, _, _)',
            'postgres.customers(customerid, _, _, fname)',
            'postgres.orderlines(olid, oid, _, _, date, _, nunits, _)',
           ],
    'condition': ['oid > 1000', 'nunits > 1'],
    'limit': '10'
    }
]
# multi source join
datalog3 = [
    {
    'result': 'Ans(pid, nunits, date, asin, nodeid)',
    'table': ['postgres.orders(oid, _, _, _, _, _, _, _, _, _, _)',
            'postgres.orderlines(olid, oid, pid, _, date, _, nunits, _)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.CategoryFlat(nodeid, category)',
           ],
    'condition': ['pid > 1000', "date > '2015-01-01'", "category = 'Education;Children & Teens'"],
    'limit': '10'
    }
]

# single source aggregation
#q(X, A) :- setof({Z}, {Y }, p(X, Y, Z), S), count(S, A)
datalog4 = [
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

# multi source aggregation
datalog5 = [
    {
    'result': 'Ans(lvl1, total_orders, total_value)',
    'table': ['postgres.orders(oid, _, _, _, _, _, _, _, _, _, _)',
            'postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.CategoryLevel(nodeid, lvl1, _, _, _, _)',
           ],
    'condition': ['pid < 10100', "date > '2015-01-01'"],
    'groupby': { 'key': 'lvl1', 'aggregation': ['count(oid, total_orders)', 'sum(price, total_value)']},
    'limit': '10'
    }
]

# union of single source
datalog6 = [
    {
    'result': 'Ans(pid, asin, total_value)',
    'table': ['postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
           ],
    'condition': ['pid < 10010'],
    'groupby': { 'key': 'pid', 'aggregation': ['sum(price, total_value)']},
    },
    {
    'result': 'Ans(pid, asin, total_value)',
    'table': ['postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
           ],
    'condition': ['pid > 10015', 'pid < 10020'],
    'groupby': { 'key': 'pid', 'aggregation': ['sum(price, total_value)']},
    },
]


# union of multi source & aggregation
datalog7 = [
    {
    'result': 'Ans(lvl1, total_orders, total_value)',
    'table': ['postgres.orders(oid, _, _, _, _, _, _, _, _, _, _)',
            'postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.CategoryLevel(nodeid, lvl1, _, _, _, _)',
           ],
    'condition': ['pid < 10100', "date > '2015-01-01'"],
    'groupby': { 'key': 'lvl1', 'aggregation': ['count(oid, total_orders)', 'sum(price, total_value)']},
    },
    {
    'result': 'Ans(lvl1, total_orders, total_value)',
    'table': ['postgres.orders(oid, _, _, _, _, _, _, _, _, _, _)',
            'postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.CategoryLevel(nodeid, lvl1, _, _, _, _)',
           ],
    'condition': ['pid > 10050', 'pid < 10200', "date > '2015-01-01'"],
    'groupby': { 'key': 'lvl1', 'aggregation': ['count(oid, total_orders)', 'sum(price, total_value)']},
    }
]

# view resolved in combiner
datalog8 = [
    {
    'result': 'temp(pid, cat, nunits, price)',
    'table': ['postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.CategoryLevel(nodeid, cat, _, _, _, _)',
           ],
    'condition': ['pid < 10100', "date > '2015-01-01'"],
    },
    {
    'result': 'Ans(cat, total_cat_value)',
    'table': ['view.temp(pid, cat, nunits, price)',
           ],
    'condition': ['nunits > 5'],
    'groupby': { 'key': 'cat', 'aggregation': ['sum(price, total_cat_value)']},
    }
]

# distinct = group by without aggregation
datalog = [
    {
    'result': 'Ans(pid)',
    'table': ['postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
           ],
    'groupby': { 'key': 'pid'},
    'limit': '10'
    }
]

# using local server, everything default
#engine = HybridEngine()
engine = HybridEngine(
                postgres= {'server': 'localhost', 'port': 5432, 'database': 'SQLBook', 'user': 'postgres', 'password': 'pavan007'},
                asterix= {'server': 'localhost', 'port': 19002, 'dataverse': 'TinySocial'},
                solr= {'server': 'localhost', 'port': 8983, 'core': 'bookstore'})
result = engine.queryDatalog(datalog, debug=True)
#result = engine.queryDatalog(datalog)
print(result)
