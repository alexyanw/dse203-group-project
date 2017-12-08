from datalog_parser_frontend import DatalogParserFE
from pprint import pprint

# single source join
datalog = '''
Ans(numunits, firstname, billdate, orderid, customerid) <-
    postgres.orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, _),
    postgres.customers(customerid, householdId, gender, firstname),
    postgres.orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, _),
    orderid > 1000, numunits > 1
'''

# single source view
datalog = '''
myview(numunits, firstname, billdate, orderid, customerid) <-
    postgres.orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, _),
    postgres.customers(customerid, householdId, gender, firstname),
    postgres.orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, _),
    orderid > 1000, numunits > 1
Ans(customerid,numunits,firstname) <-
    view.myview(numunits,firstname,_,_,customerid)
'''

# single source union
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

# single source aggregation
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

# union of multi source & aggregation
datalog = '''
cat_sum1(lvl1, oid, nunits, price) <-
  postgres.orderlines(olid, oid, pid, _, date, _, nunits, price),
  postgres.products(pid, _, _, _, _, _, asin, nodeid),
  asterix.categorylevel(nodeid, lvl1, _, _, _, _),
  pid < 10100, date > '2015-01-01'
ans(lvl1, total_value) <-
  setof({oid}, {nunits}, {price}, cat_sum1(lvl1, oid, nunits, price), S), sum(nunits, total_units),sum(price,total_value)
cat_sum2(lvl1, oid, nunits, price) <-
  postgres.orderlines(olid, oid, pid, _, date, _, nunits, price),
  postgres.products(pid, _, _, _, _, _, asin, nodeid),
  asterix.categorylevel(nodeid, lvl1, _, _, _, _),
  pid > 10050, pid < 10200
ans(lvl1, total_value) <-
  setof({oid}, {nunits}, {price}, cat_sum2(lvl1, oid, nunits, price), S), sum(nunits, total_units),sum(price,total_value)
'''

fe = DatalogParserFE()
result = fe.parse(datalog)
pprint(result)
