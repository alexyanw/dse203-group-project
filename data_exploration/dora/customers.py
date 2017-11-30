from .datasources import SqlSource, QueryResponse
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
    def statsByCustomer(self, min_date='1900-1-1', max_date=None, householdid=[], sample_size=100):
        """For each customer, find the number of books orders, gender, zipcode, household,
           first name, and total spend on books. 

        Args:
            min_date (string): optional. date. Limits the search result timeframe.
            max_date (string): optional. date. Limits the timeframe for which 
            customers results will be returned
            householdid (list): optional. householdids that will be excluded from the query results.
            sample_size (int): optional. Percentage of the data the query will run over.
        
        Returns:
             tuple(numOrders, gender, zipcode, householdid, firstname, TotalSpent): numOrders 
             is the number of times a customer has purchased a book. gender is the gender of the 
             customer. zipcode identifiies the customers location. householdid is the customer's 
             hosuehold identification. firstname is the customer's name. TotalSpent is the total 
             amount the customer has spent on books. 
        """

        max_date_filter = ' AND o.orderdate <= %(max_date)s ' if max_date else ' '
        household_filter = (' AND c.householdid not in %(householdid_list)s '
                            if (householdid is not None) & (len(householdid) > 0)
                            else ' ')

        query = ('''
            SELECT
                count(o.orderid) as numOrders,
                CASE
                    WHEN c.gender = 'M'
                    THEN 0
                    WHEN c.gender = 'F'
                    THEN 1
                    ELSE 2
                END AS gender,
                o.zipcode::integer,
                sum(regexp_replace(o.totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as TotalSpent,
                c.householdid,
                c.firstname
            FROM
                customers c,
                orders o
            WHERE
                c.customerid!=0
                AND LENGTH(o.zipcode) >= 5
                AND LENGTH(o.zipcode) < 7
                AND o.zipcode  ~ '^\d+(.\d+)?$'
                AND o.orderdate >= %(min_date)s'''
                 + household_filter
                 + max_date_filter
            + '''GROUP BY
                    c.gender,
                    c.householdid,
                    o.zipcode,
                    c.firstname
            ORDER BY numOrders desc''')

        return self._execSqlQuery(query,
              {
                  'min_date': min_date,
                  'max_date': max_date,
                  'householdid_list': tuple(householdid),
                  'sample_size': sample_size,
                  'random_seed': self._random_seed
              })

    @log
    def clusterCustomers(self,
                         feature_set=None,
                         n_clusters=0,
                         algorithm='auto',
                         init='k-means++',
                         cluster_on=['numorders', 'gender', 'zipcode', 'totalspent'],
                         scale=False):
        """Clusters the customers together based on gender, zipcode, numOrders, and TotalSpent. 

        Args:
            feature_set (QueryResponse or dictionary): optional. must have keys 'results' and 'columns'. Data that will be clustered.
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
            cluster_on (list of str): column names to use as cluster features
        
        Returns:
             tuple(clusteringScaled, clustering, centers): clusteringScaled is a dataframe of the
             inputdata normalized and the predicted labels for the cluster. clustering is a dataframe 
             of the data and has a y_pred column which is the cluster label for the customer. centers 
             is an array of the cluster centers.
        """
        if feature_set is None:
            feature_set = self.statsByCustomer()

        if ('results' not in feature_set.keys()) | ('columns' not in feature_set.keys()):
            raise 'invalid feature set, no results or columns'

        data = pd.DataFrame(feature_set.results, columns=feature_set.columns)
        response_columns = feature_set.columns + ['cluster']

        X = (
            data[cluster_on].values
            if scale is False
            else StandardScaler().fit_transform(data[cluster_on].values)
        )

        clustering = pd.DataFrame(X, columns=cluster_on)

        algorithm = KMeans(n_clusters=(n_clusters), algorithm=(algorithm), init=(init))
        algorithm.fit_predict(X)

        for col in feature_set.columns:
            if col not in clustering.columns.values:
                clustering.loc[:, col] = data[col]

        clustering.loc[:, 'cluster'] = algorithm.labels_

        centers = algorithm.cluster_centers_

        response = QueryResponse(
            columns=response_columns,
            results=[tuple(x) for x in clustering.values]
        )

        return response
