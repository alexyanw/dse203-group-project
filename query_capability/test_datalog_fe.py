from datalog_parser_frontend import DatalogParserFE

datalog = '''
Ans(numunits, firstname, billdate, orderid, customerid) <-
    postgres.orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, _),
    postgres.customers(customerid, householdId, gender, firstname),
    postgres.orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, _),
    orderid > 1000, numunits > 1
'''

datalog = '''
myview(numunits, firstname, billdate, orderid, customerid) <-
    postgres.orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, _),
    postgres.customers(customerid, householdId, gender, firstname),
    postgres.orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, _),
    orderid > 1000, numunits > 1
Ans(customerid,numunits,firstname) <-
    view.myview(numunits,firstname,_,_,customerid)
'''

datalog = '''
Ans(numunits, billdate, orderid, customerid) <-
    postgres.orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, _),
    postgres.orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, _),
    orderid > 1000, numunits > 1
Ans(numunits, billdate, orderid, customerid) <-
    postgres.orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, _),
    postgres.orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, _),
    orderid < 1000, unitPrice > 100,
    order by numunits DESC
'''

datalog = '''
cust_prod_view(cid, pid, oid, price) <-
  postgres.orders(oid, cid, _, _, _, _, _, _, _, _, _),
  postgres.orderlines(olid, oid, pid, _, date, _, nunits, price),
  postgres.products(pid, _, _, _, _, _, asin, nodeid),
  postgres.customers(cid, _, _, _),
  nunits > 1
ans(pid, total_value) <-
  setof({cid}, {oid}, {price}, cust_prod_view(cid, pid, oid, price), S), count(price, total_orders),sum(price,total_value)
'''

fe = DatalogParserFE()
result = fe.parse(datalog)
print(result)
#print(result[0]['table'])
