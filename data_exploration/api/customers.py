from util.sql_source import SqlSource

class Customers(SqlSource):

    def statsByHousehold(self, min_date=None, max_date=None, sample_size=100, order_by='TotalOrders'):
        return self._execSqlQuery('''
	    SELECT
	        customers.householdid as HouseholdID,
                sum(orders.totalprice) as TotalSpent,
	        count(orders) as TotalOrders,
                min(orderdate) as first_order,
                max(orderdate) as last_order,
                age(min(orderdate)) as time_as_customer,
                age(max(orderdate)) as time_since_last_order
            FROM orders, customers 
            WHERE orders.customerid=customers.customerid and customers.customerid != 0
            GROUP BY HouseholdID order by {} DESC'''.format(order_by))

    def membersOfHousehold(self, min_date=None, max_date=None, sample_size=100 householdID=0):
        return self._execSqlQuery('''
	    SELECT
                customerid, firstname, gender 
            FROM customers 
            WHERE householdid={}'''.format(householdID))

    def productsByHousehold(self, min_date=None, max_date=None, sample_size=100 householdID=0):
        return self._execSqlQuery('''
	    SELECT
	        distinct(products.productid), products.ASIN
            FROM orderlines, products, orders, customers
            WHERE
                orderlines.productid = products.productid AND 
                orderlines.orderid = orders.orderid AND
                orders.customerid = customers.customerid AND
                customers.householdid={}'''.format(householdID))
