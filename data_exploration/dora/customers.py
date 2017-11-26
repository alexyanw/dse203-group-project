from .datasources import SqlSource
from .logger import log
from datetime import datetime

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

    def membersOfHousehold(self, min_date=None, max_date=None, sample_size=100, householdID=0):
        return self._execSqlQuery('''
	    SELECT
                customerid, firstname, gender 
            FROM customers 
            WHERE householdid={}'''.format(householdID))

    def productsByHousehold(self, min_date=None, max_date=None, sample_size=100, householdID=0):
        return self._execSqlQuery('''
	    SELECT
	        distinct(products.productid), products.ASIN
            FROM orderlines, products, orders, customers
            WHERE
                orderlines.productid = products.productid AND 
                orderlines.orderid = orders.orderid AND
                orders.customerid = customers.customerid AND
                customers.householdid={}'''.format(householdID))
    
    @log
    def clusterQuery(self,min_date='1900-1-1', max_date=None):
        """For each customer, find the number of books orders, gender, zipcode, household,
           first name, and total spend on books. 

        Args:
            max_date (string): optional. date. Limits the timeframe for which 
            customers results will be returned
        
        Returns:
             tuple(numOrders, gender, zipcode, householdid, firstname, TotalSpent): numOrders 
             is the number of times a customer has purchased a book. gender is the gender of the 
             customer. zipcode identifiies the customers location. householdid is the customer's 
             hosuehold identification. firstname is the customer's name. TotalSpent is the total 
             amount the customer has spent on books. 
        """
        
        max_date_filter = ' AND o.orderdate <= %(max_date)s' if max_date else ' '
        query = ( '''
                  SELECT count(o.orderid) as numOrders, 
                          c.gender, 
                          o.zipcode,
                          c.householdid,
                          c.firstname,
                          sum(o.totalprice) as TotalSpent
                  FROM customers c, orders o
                  WHERE c.customerid!=0 AND o.customerid=c.customerid 
                  GROUP BY c.gender, c.householdid, o.zipcode
                  ORDER BY numOrders desc''')
        return self._execSqlQuery(query)
    
    @log
    def clusterCustomers(self,n_clusters,algorithm):
        """Clusters the customers together based on gender, zipcode, numOrders, and TotalSpent. 

        Args:
            n_clusters (int): required. The number of clusters to form as well 
            as the number of centroids to generate.
            algorithm (string): required. “auto”, “full” or “elkan”. K-means algorithm to use. 
            The classical EM-style algorithm is “full”. The “elkan” variation is more efficient 
            by using the triangle inequality, but currently doesn’t support sparse data. “auto” 
            chooses “elkan” for dense data and “full” for sparse data.
        
        Returns:
             tuple(householdid, firstname, y_pred): householdid is the customer's hosuehold
             identification. firstname is the customer's name. y_pred is the cluster label for 
             that customer.
        """
        
        data=self.clusterQuery()
        data=pd.DataFrame(data, data.columns)
        X=data[['numOrders, gender, zipcode, TotalSpent']].values
        X=StandardScaler().fit_transform(X)
        algorithm=KMeans(n_clusters=(n_clusters), algorithm=(algorithm))
        algorithm.fit_predict(X)
        y_pred=algorithm.labels_
        customer=data[['householdid','firstname']]
        clustering=zip(customer,y_pred)
        
        return clustering
