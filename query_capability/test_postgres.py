from postgres_engine import PostgresEngine
#from datalog_parser import DatalogParser

datalog1 = [
    'Ans(numunits, firstname, billdate, orderid, customerid)',
    'orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, numUnits)',
    'customers(customerid, householdId, gender, firstname)',
    'orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, totalPrice)',
    ['orders.orderid > 1000', 'orders.numunits > 1']
]
datalog2 = [
    'Ans(productid, total_review_count)',
    'product_view(productid, nodeid, fullprice, isinstock, total_review_count, avg_review_rating , total_order_count , total_copy_count )'
]
datalog3 = [
    'Ans(productid, fullprice)',
    'products(productid, nodeid, fullprice, isinstock)'
    ]
datalog4 = [
    'Ans(productid, total_order_count)',
    'product_orders(productid, total_order_count, total_copy_count)'
    ]

datalog5 = [
    'Ans(product_a, product_b, pair_count)',
    'cooccurrence_matrix(product_a, product_b, pair_count)'
    ]
datalog6 = [
    'Ans(numunits, firstname, billdate, orderid, customerid)',
    'orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, numUnits)',
    'customers(customerid, householdId, gender, firstname)',
    'not customers(customerid, householdId, gender, "John")',
    'orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, totalPrice)',
    ]
# join
datalog7 = [
    'Ans(product_a, product_b, pair_count)',
    'cooccurrence_matrix(product_a, product_b, pair_count)',
    'product_view(product_a, _, fullprice, _, _, _ , total_order_count , total_copy_count)',
    'NOT product_view(product_a, _, _, 0, _, _ , _ , _)',
    ['total_order_count > 10', 'fullprice < 100']
    ]

# view, aggregation
datalog = [
    'CoMat(product_a, product_b, pair_count)',
    'cooccurrence_matrix(product_a, product_b, pair_count)',
    'product_view(product_a, _, fullprice, _, _, _ , total_order_count , total_copy_count)',
    'NOT product_view(product_a, _, _, 0, _, _ , _ , _)',
    ['total_order_count > 10', 'fullprice < 100'],
    'q(product_a, A) :- setof({pair_count}, E^b. CoMat(product_a, b, pair_count), S), sum(S, A)'
    ]

postgres = PostgresEngine()
sql = postgres.queryDatalog(datalog, debug=True)
print(sql)
