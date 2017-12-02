from .datasources import SqlSource, QueryResponse
from .logger import log
from datetime import datetime
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import numpy as np

class Customers(SqlSource):
    @log
    def statsByHousehold(self, min_date='1900-1-1', max_date=None, sample_size=100):
        """For each household find the total amount spent, the total number of order, the orderdate of
        thefirst and last order, the time spent as customer and the time since the last order.  

        Args:
            min_date (string): optional. date. Limits the search result timeframe.
            max_date (string): optional. date. Limits the timeframe for which 
            customers results will be returned
            sample_size (int): optional. Percentage of the data the query will run over.
        
        Returns:
             tuple(householdid, TotalSpent, TotalOrders, first_order, last_order, time_as_customer,
             time_since_last_order): householdid is the unique household id. TotalSpent is the amount of
             money spenton all order. TotalOrders is the number of orders that have been made by that
             household. first_order is the time when the first order was made. last_order is the time
             since the lastorder. time_as_customer is the time the members of the household has been
             customers. """
        
        max_date_filter = ' AND o.orderdate <= %(max_date)s ' if max_date else ' '
        
        query=('''
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
                '''
                + max_date_filter
                + '''
                AND orders.orderdate>%(min_date)s
            GROUP BY HouseholdID
            ORDER By count(orders) DESC''')

        return self._execSqlQuery(query,
            {
                'min_date': min_date,
                'max_date': max_date,
                'sample_size': sample_size,
                'random_seed': self._random_seed
            })

    @log
    def membersOfHousehold(self,householdID=0, sample_size=100):
        """For each household, find the customerid, firstname, and gender for each member. 
        
        Args:
            householdID (stirng): optional. The householdID that the members will be found of.
            sample_size (int): optional. Percentage of the data the query will run over.
        
        Returns:
             tuple(customerid, firstname, gender): customerid is the unique customer id. firstname is the
             name of the customer. gender is the gender of the customer. """
        
        query=('''
	    SELECT
                customerid, firstname, gender 
            FROM customers 
            WHERE householdid=%(householdID)s''')
        return self._execSqlQuery(query,
            {
                'householdID':householdID,
                'sample_size': sample_size,
                'random_seed': self._random_seed
            })

    @log
    def productsByHousehold(self, min_date='1900-1-1', max_date=None, householdID=0, sample_size=100):
        """For each household, all of the products that have been purchased.  
        
        Args:
            min_date (string): optional. date. Limits the search result timeframe.
            max_date (string): optional. date. Limits the timeframe for which 
            customers results will be returned
            householdID (stirng): optional. The householdID that the members will be found of.
            sample_size (int): optional. Percentage of the data the query will run over.
        
        Returns:
             tuple(productid, asin, gender): product is the unique product id. asin is the product code
             of the product that was purchased. """
        
        max_date_filter = ' AND orders.orderdate <= %(max_date)s ' if max_date else ' '
        
        query=('''
	    SELECT
	        distinct(products.productid), products.ASIN
            FROM orderlines, products, orders, customers
            WHERE
                orderlines.productid = products.productid AND 
                orderlines.orderid = orders.orderid AND
                orders.customerid = customers.customerid AND
                customers.householdid=%(householdID)s AND
                orders.orderdate > %(min_date)s
                '''
                + max_date_filter
                + ''' 
                ''')
        return self._execSqlQuery(query,
            {
                'min_date': min_date,
                'max_date': max_date,
                'householdID':householdID,
                'sample_size': sample_size,
                'random_seed': self._random_seed
            })

    @log
    def idsForCustomer(self, customermatchedids = []):
        return self._execSqlQuery('''
            SELECT *
            FROM customers_matched_customerids
            WHERE customermatchedid in %(customermatchedids)s''',
            {
                'customermatchedids': tuple(customermatchedids),
            })

    @log
    def statsByCustomer(self, min_date='1900-1-1', max_date=None, householdid=[], sample_size=100):
        """For each customer, find the number of books orders, gender, zipcode, household,
           first name, and total spend on books. 

        Args:
            min_date (string): optional. date. Limits the search result timeframe.
            max_date (string): optional. date. Limits the timeframe for which 
            customers results will be returned
            householdid (tuple): optional. householdids that will be excluded from the query results.
            sample_size (int): optional. Percentage of the data the query will run over.
        
        Returns:
             tuple(numOrders, gender, zipcode, TotalPop, MedianAge, TotalMales, TotalFemales,
             TotalSpent,householdid, firstname,numCustomerid ): numOrders is the number of times a
             customer has purchased a book. gender is the gender of the customer. zipcode identifiies 
             the customers location.  TotalPop is the total population for the zipcode. MedianAge is 
             the median age of the population for the zipcode. TotalMales is the total number of males 
             of the population for the zipcode. TotalFemales is the total number of females of the
             population for the zipcode. TotalSpent is the total amount the customer has spent on books. 
             householdid is the customer's hosuehold identification. firstname is the customer's name.
             numCustomerid is the number of customerids per customer.
        """
        
        max_date_filter = ' AND o.orderdate <= %(max_date)s ' if max_date else ' '
        household_filter = (' AND c.householdid not in %(householdid_list)s '
                            if (householdid is not None) & (len(householdid) > 0)
                            else ' ')

        query = ('''
            SELECT
                cmc.customermatchedid,
                count(o.orderid) as numOrders,
                CASE
                    WHEN c.gender = 'M'
                    THEN 0
                    WHEN c.gender = 'F'
                    THEN 1
                    ELSE 2
                END AS gender,
                TRIM(o.zipcode)::NUMERIC as zipcode,
                z.totpop as TotalPop,
                z.medianage as MedianAge,
                z.males as TotalMales,
                z.females as TotalFemales,
                sum(regexp_replace(o.totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as TotalSpent,
                c.householdid,
                c.firstname,
                count(c.customerid) as numCustomerid
            FROM
                customers c,
                customers_matched cm,
                customers_matched_customerids cmc,
                orders o,
                zipcensus z
            WHERE
                c.customerid!=0
                AND o.customerid=c.customerid
                AND c.customerid = cmc.customerid
                AND cmc.customermatchedid = cm.customermatchedid
                AND LENGTH(trim(o.zipcode)) >= 5
                AND LENGTH(trim(o.zipcode)) < 7
                AND trim(o.zipcode)  ~ '^\d+$' '''
                + household_filter
                + max_date_filter
                + '''
                AND o.zipcode=z.zcta5
            GROUP BY
                cmc.customermatchedid,
                c.gender,
                c.householdid,
                trim(o.zipcode),
                z.totpop,
                z.medianage,
                z.males,
                z.females,
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
                         n_clusters=8,
                         algorithm='auto',
                         init='k-means++',
                         cluster_on=[
                             'numorders',
                             'gender',
                             'totalpop',
                             'totalspent',
                             'zipcode',
                             'medianage',
                             'totalmales',
                             'totalfemales'
                         ],
                         scale=False):
        """Clusters the customers together based on gender, zipcode, numOrders, and TotalSpent. 
        Args:
            feature_set (QueryResponse or dictionary): optional. must have keys 'results' and 'columns'.
            Data that will be clustered.
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
             tuple(numorders', 'gender', 'totalpop', 'totalspent', 'zipcode', 'customermatchedid',
             'medianage', 'totalmales', 'totalfemales', 'householdid', 'firstname', 'numcustomerid',
             'cluster', 'customerids): numOrders is the number of times a customer has purchased a book.
             gender is the gender of the customer. zipcode identifiies the customers location.  TotalPop
             is the total population for the zipcode. MedianAge is the median age of the population for
             the zipcode. customermatchedid is the number of customerids that matched with the customer.
             TotalMales is the total number of males of the population for the zipcode. TotalFemales is
             the totalnumber of females of the population for the zipcode. TotalSpent is the total amount
             the customer has spent on books. householdid is the customer's hosuehold identification.
             firstname is the customer's name.numCustomerid is the number of customerids per customer.
             cluster is the label of the cluster the customer belongs to. customerids is a list of all
             the customerids that correspond to that customer. 
        """
        if feature_set is None:
            feature_set = self.statsByCustomer()
        if (not hasattr(feature_set, 'results')) | (not hasattr(feature_set, 'columns')):
            raise Exception('invalid feature set, no results or columns')



        data = pd.DataFrame(feature_set.results, columns=feature_set.columns)
        data[cluster_on] = data[cluster_on].apply(pd.to_numeric, errors='coerce', axis=1)
        if (data[cluster_on].isnull().values.any())==True:
            data=data.dropna()

        X = (
            data[cluster_on].values
            if scale is False
            else StandardScaler().fit_transform(data[cluster_on].values)
        )

        clustering = pd.DataFrame(X, columns=cluster_on)

        def label_custids(clustering_row, custid_response):
            matched_index = custid_response.columns.index('customermatchedid')
            custid_index = custid_response.columns.index('customerid')
            return [ x[custid_index]
                for x in custid_response.results
                if x[matched_index] == clustering_row['customermatchedid']]

        algorithm = KMeans(n_clusters=(n_clusters), algorithm=(algorithm), init=(init))
        algorithm.fit_predict(X)

        for col in feature_set.columns:
            if col not in clustering.columns.values:
                clustering.loc[:, col] = data[col]
        

        response_columns=np.append(clustering.columns.values,'cluster')
        clustering.loc[:, 'cluster'] = algorithm.labels_

        response_columns = np.append(response_columns, 'customerids')
        customerids = self.idsForCustomer(clustering['customermatchedid'])
        custid_df = (pd.DataFrame(
            data=customerids.results,
            columns=customerids.columns)
                     .groupby('customermatchedid')
                     .aggregate(lambda x: tuple(x))
                     .reset_index())

        clustering = pd.merge(clustering, custid_df, on='customermatchedid', how='left')

        centers = algorithm.cluster_centers_

        response = QueryResponse(
            columns=response_columns,
            results=[tuple(x) for x in clustering.values]
        )

        return response