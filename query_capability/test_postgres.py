from hybrid_engine import HybridEngine

datalog = [{
    'result': 'Ans(productid, fullprice)',
    'table': ['postgres.products(productid, nodeid, fullprice, isinstock)']
    }]

datalog1 = [{
    'result': 'Ans(customerid, productid, numunits, billdate)',
    'table': ['postgres.orders(oid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, numUnits)',
            'postgres.customers(customerid, householdId, gender, firstname)',
            'postgres.orderlines(orderLineId, oid, productId, shipDate, billdate, unitPrice, numunits, totalPrice)'],
    'condition': ['oid > 1000', 'numunits > 1']
    }]

# predefined views: product_view
datalog2 = [{
    'result': 'Ans(productid, total_review_count)',
    'table': ['postgres.product_view(productid, nodeid, fullprice, isinstock, total_review_count, avg_review_rating , total_order_count , total_copy_count )']
    }]

# predefined views: product_orders
datalog3 = [{
    'result': 'Ans(productid, total_order_count)',
    'table': ['postgres.product_orders(productid, total_order_count, total_copy_count)']
    }]

# predefined views: coocurrence_matrix
datalog4 = [{
    'result': 'Ans(product_a, product_b, pair_count)',
    'table': ['postgres.cooccurrence_matrix(product_a, product_b, pair_count)']
    }]

# negation
datalog5 = [{
    'result': 'Ans(numunits, firstname, billdate, orderid, customerid)',
    'table': ['postgres.orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, numUnits)',
            'postgres.customers(customerid, householdId, gender, firstname)',
            'not postgres.customers(customerid, householdId, gender, "John")',
            'postgres.orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, totalPrice)'],
    }]

# join
datalog6 = [{
    'result': 'Ans(product_a, product_b, pair_count)',
    'table': ['postgres.cooccurrence_matrix(product_a, product_b, pair_count)',
            'postgres.product_view(product_a, _, fullprice, _, _, _ , total_order_count , total_copy_count)'],
    'condition': ['total_order_count > 10', 'fullprice < 100']
    }]

# view, aggregation
datalog = [{
    'result': 'CoMat(product_a, product_b, pair_count)',
    'table': ['postgres.cooccurrence_matrix(product_a, product_b, pair_count)',
            'postgres.product_view(product_a, _, fullprice, isinstock, _, _ , total_order_count , total_copy_count)',
            'not postgres.product_view(product_a, _, _, "N", _, _ , _ , _)'
            ],
    'condition': ['total_order_count > 1', 'fullprice < 100'],
    },
    {
        'result': 'Ans(product_a, total_co_cocunt)',
        'table': ['view.CoMat(product_a, product_b, pair_count)'],
        'groupby': {'key': 'product_a', 'aggregation': ['sum(pair_count, total_co_count)']}
        #'q(product_a, A) :- setof({pair_count}, E^b. CoMat(product_a, b, pair_count), S), sum(S, A)'
    }]

engine = HybridEngine(
                postgres= {'server': 'localhost', 'port': 5432, 'database': 'SQLBook', 'user': 'postgres', 'password': 'pavan007'},
                asterix= {'server': 'localhost', 'port': 19002, 'dataverse': 'TinySocial'},
                solr= {'server': 'localhost', 'port': 8983, 'core': 'bookstore'})

#result = engine.queryDatalog(datalog, debug=True)
result = engine.queryDatalog(datalog)
print(result)
