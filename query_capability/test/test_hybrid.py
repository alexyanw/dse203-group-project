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
    'limit': '10'
    }
]

# single source with column renaming
datalog = [
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
datalog = [
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
datalog = [
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
datalog = [
    {
    'result': 'Ans(lvl1, total_orders, total_value)',
    'table': ['postgres.orders(oid, _, _, _, _, _, _, _, _, _, _)',
            'postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.CategoryLevel(nodeid, lvl1, _, _, _, _)',
           ],
    'condition': ['pid > 1000', "date > '2015-01-01'"],
    'groupby': { 'key': 'lvl1', 'aggregation': ['count(oid, total_orders)', 'sum(price, total_value)']},
    'limit': '10'
    }
]

# union
# view

engine = HybridEngine()
#result = engine.queryDatalog(datalog, debug=True)
result = engine.queryDatalog(datalog)
print(result)
