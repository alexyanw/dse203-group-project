from .datasources import SqlSource
from .logger import log
from datetime import datetime
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

class Products(SqlSource):
    @log
    def ratingsDistribution(self, min_date='1900-1-1', max_date=None, sample_size=100, asin=[]):

        max_date_filter =  ' AND r.ReviewTime <= %(max_date)s' if max_date else ' '

        asin_filter = ' AND r.asin IN %(asin_list)s ' if len(asin) > 0 else ' '

        query = ('''
            WITH CTE as (
              SELECT
                p.asin,
                p.productid,
                r.reviewtime,
                r.overall
              FROM products p
              LEFT JOIN reviews r
                TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s)
                ON p.asin = r.asin
              WHERE
                r.ReviewTime >= %(min_date)s'''
                + max_date_filter
                + asin_filter
                + '''ORDER BY r.overall
            )  SELECT
                asin,
                productid,
                SUM(
                    CASE
                      WHEN overall = 1
                      THEN 1
                      ELSE 0
                    END) as one_star_votes,
                SUM(
                    CASE
                      WHEN overall = 2
                      THEN 1
                      ELSE 0
                    END) as two_star_votes,
                SUM(
                    CASE
                      WHEN overall = 3
                      THEN 1
                      ELSE 0
                    END) as three_star_votes,
                SUM(
                    CASE
                      WHEN overall = 4
                      THEN 1
                      ELSE 0
                    END) as four_star_votes,
                SUM(
                    CASE
                      WHEN overall = 5
                      THEN 1
                      ELSE 0
                    END) as five_star_votes
              FROM CTE
              GROUP BY asin, productid
              ORDER BY five_star_votes DESC''')

        return self._execSqlQuery(query,
            {
                'min_date':min_date,
                'max_date':max_date,
                'asin_list':tuple(asin),
                'sample_size':sample_size,
                'random_seed':self._random_seed
            })

    @log
    def coPurchases(self, asin, min_date='1900-1-1', max_date=None, sample_size=100 ):
        """For each given book, find all the books purchased in the same order as the given book 
        and the number of times that book was purchased.

        Args:
            asin (list): required. book asin ids. Determines the books that coPurchases will 
            be searched for. 
            min_date (string): optional. date. Limits the search result timeframe.
            max_date (string): optional. date. Limits the search result timeframe.
            sample_size (int): optional. Percentage of the data the query will run over.
        
        Returns:
             tuple(asin, numPurch): asin is the identification of the book that was purchased 
             in the same order as one of the input bools. numPurch is the number of times 
             the book the book was purchased.   
        """
        
        if len(asin) == 0: 
            return None
        
        max_date_filter = ' AND o.orderdate <= %(max_date)s ' if max_date else ' '

        query = ('''
            SELECT
              p.asin,
              count(ol.productid) as numPurch
            FROM
              products p,
              orderlines ol
              TABLESAMPLE SYSTEM(%(sample_size)s) REPEATABLE(%(random_seed)s),
              orders o
            WHERE
              o.orderid = ol.orderid
              AND o.orderdate > %(min_date)s'''+max_date_filter+'''
              AND ol.orderid in (
                SELECT orid.orderid
                FROM (
                  SELECT
                    orderlines.orderid,
                    orderlines.productid
                  FROM
                    orderlines,
                    (
                      SELECT products.productid
                      FROM products
                      WHERE products.asin in %(asin_list)s
                    ) as pid
                  WHERE orderlines.productid=pid.productid
                ) as orid
                WHERE
                  orid.orderid in (
                    SELECT orderlines.orderid
                    FROM orderlines
                    GROUP BY orderlines.orderid
                    having count(orderlines.orderid) >1
                  )
              )
              AND ol.productid=p.productid
              AND p.asin not in %(asin_list)s
            GROUP BY p.asin
            ORDER BY numPurch DESC;''')
        return self._execSqlQuery (query,
               {
                    'min_date':min_date,
                    'max_date':max_date,
                    'asin_list':tuple(asin),
                    'sample_size':sample_size,
                    'random_seed':self._random_seed
               })
        
    @log
    def clusterQuery(self, min_date='1900-1-1', max_date=None, sample_size=100):
        """For each book the product id, asin, the number of times it was purchased, 
        the average star rating, the product category, and days the product has been on sale 
        is returned.

        Args: 
            min_date (string): optional. date. Limits the search result timeframe .
            max_date (string): optional. date. Limits the search result timeframe.
            sample_size (int): optional. Percentage of the data the query will run over.
        
        Returns:
             tuple(productid, asin, num_orders, avgrating, category, days_on_sale): productid is 
             the products unqiue identifier. asin is the identification of the book. num_orders 
             counts the number of times the book has been purchased. avgrating is the average star 
             rating of the book based on the user reviews. category is the product category that the 
             book belongs to. days_on_sale is the number of days the book has been on sale.
        """
        
        max_date_filter = ' AND o.orderdate <= %(max_date)s' if max_date else ' '
        query = ('''
                  SELECT orderlines.productid,
                      products.asin,
                      count(*) as num_orders,
                      avg(r.overall) as avgrating,
                      products.nodeid as category,
                      DATE_PART('day', max(shipdate)::TIMESTAMP - min(shipdate)::TIMESTAMP) as days_on_sale
                  FROM orderlines
                      JOIN products
                        ON orderlines.productid = products.productid
                      JOIN orders o
                        ON orderlines.orderid = o.orderid
                      JOIN reviews r
                        ON products.asin=r.asin
                      JOIN calendar
                        ON o.orderdate=calendar.date
                  WHERE orderlines.numunits > 0
                  group by orderlines.productid, products.asin, products.nodeid;''')
        return self._execSqlQuery(query,
               {
                    'min_date':min_date,
                    'max_date':max_date,
                    'sample_size':sample_size,
                    'random_seed':self._random_seed
               })
        
    @log
    def clusterProducts(self,n_clusters=8,algorithm='auto',asin=None):
        """Clusters the books together using KMeans clustering utilizing the clusterQuery 
        results as the features (num_orders, avgrating, category, and days_on_sale).

        Args: 
            num_clusters (int): optional. default=8 The number of clusters to form as well as 
            the number of centroids to generate.
            algorithm (string): optional. “auto”, “full” or “elkan”, default=”auto”. K-means algorithm 
            to use. The classical EM-style algorithm is “full”. The “elkan” variation is more efficient
            by using the triangle inequality, but currently doesn’t support sparse data. “auto” chooses
            “elkan” for dense data and “full” for sparse data.
        
        Returns:
             tuple(productid, asin, y_pred): productid is the products unqiue identifier. asin is the
             identification of the book. y_pred is the label of the clsuter that the product belongs to.
        """
        response=self.clusterQuery()
        data=pd.DataFrame(response.results)
        data.columns=response.columns
        if asin is None:
            input_centers='k-means++'
            X=data[['num_orders','avgrating','category','days_on_sale']].values
        else:
            #input_centers=data.loc[data['asin'].isin([%(asin_list)s])]
            asin_s=set(asin)
            input_centers=data[data['asin'].isin(asin)]
            input_centers=input_centers[['num_orders','avgrating','category','days_on_sale']].values
            #X=data.loc[~data['asin'].isin([%(asin_list)])]
            data=data[~data['asin'].isin(asin)]
            X=data[['num_orders','avgrating','category','days_on_sale']].values
        X=StandardScaler().fit_transform(X)
        #algorithm=KMeans(n_clusters=%(n_clusters), algorithm=%(algorithm),init=input_centers)
        algorithm=KMeans(n_clusters=(n_clusters), algorithm=(algorithm),init=input_centers)
        algorithm.fit_predict(X)
        y_pred=algorithm.labels_
        clustering=data[['productid','asin']]
        clustering.loc[:,'y_pred']=y_pred
        
        return clustering
    
    
                                          
        
        