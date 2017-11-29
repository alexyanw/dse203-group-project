from .datasources import SqlSource
from .logger import log
from datetime import datetime
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

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
    def clusterQuery(self,min_date='1900-1-1', max_date=None, sample_size=100):
        """For each customer, find the number of books orders, gender, zipcode, household,
           first name, and total spend on books. 

        Args:
            min_date (string): optional. date. Limits the search result timeframe.
            max_date (string): optional. date. Limits the timeframe for which 
            customers results will be returned
            sample_size (int): optional. Percentage of the data the query will run over.
        
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
                  GROUP BY c.gender, c.householdid, o.zipcode, c.firstname
                  ORDER BY numOrders desc''')
        return self._execSqlQuery(query,
              {
                    'min_date':min_date,
                    'max_date':max_date,
                    'sample_size':sample_size,
                    'random_seed':self._random_seed
               })
    
    @log
    def clusterCustomers(self,n_clusters=0,algorithm='auto', init='k-means++'):
        """Clusters the customers together based on gender, zipcode, numOrders, and TotalSpent. 

        Args:
            num_clusters (int): optional. default=8 The number of clusters to form as well as 
            the number of centroids to generate.
            algorithm (string): optional. “auto”, “full” or “elkan”, default=”auto”. K-means algorithm 
            to use. The classical EM-style algorithm is “full”. The “elkan” variation is more efficient
            by using the triangle inequality, but currently doesn’t support sparse data. “auto” chooses
            “elkan” for dense data and “full” for sparse data.
            init (string): optional. {‘k-means++’, ‘random’ or an ndarray}. Method for initialization,
            defaults to ‘k-means++’:‘k-means++’ : selects initial cluster centers for k-mean 
            clustering in a smart way to speed up convergence. See section Notes in k_init for more
            details.
            ‘random’: choose k observations (rows) at random from data for the initial centroids.
            If an ndarray is passed, it should be of shape (n_clusters, n_features) and gives the 
            initial centers.
        
        Returns:
             tuple(householdid, firstname, y_pred): householdid is the customer's hosuehold
             identification. firstname is the customer's name. y_pred is the cluster label for 
             that customer.
        """
        
        response=self.clusterQuery()
        data=pd.DataFrame(response.results, columns=response.columns)
        mask = (data['zipcode'].str.len()>=5) & (data['zipcode'].str.len()<7) & (data['zipcode'].str.isnumeric())
        data = data.loc[mask]
        data.loc[data.gender=='F','gender']=1
        data.loc[data.gender=='M','gender']=0
        data.loc[data.gender=='','gender']=2
        data['zipcode']=data['zipcode'].apply(pd.to_numeric)
        X=data[['numorders', 'gender', 'zipcode', 'totalspent']].values
        for i in range(len(X)):
            X[i][3]=X[i][3].replace(",", "")
            X[i][3]=float(X[i][3].strip('$'))
        X=StandardScaler().fit_transform(X)
        algorithm=KMeans(n_clusters=(n_clusters), algorithm=(algorithm), init=(init))
        algorithm.fit_predict(X)
        y_pred=algorithm.labels_
        clustering=data[['householdid','gender']]
        clustering.loc[:,'y_pred']=y_pred

        return clustering
