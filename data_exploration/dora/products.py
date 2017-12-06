from .datasources import SqlSource, QueryResponse
from .logger import log
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import numpy as np
from sklearn.decomposition import PCA


class Products(SqlSource):
    def _createPriceBins(self, bins):
        bin_selects = []
        bin_ranges = []

        if type(bins) is list:
            bin_ranges = bins
        elif type(bins) is int:

            range_response = self._execSqlQuery('''
                SELECT
                    COALESCE(MAX(unitprice::Numeric),0) as max,
                    COALESCE(MIN(unitprice::Numeric),0) as min
                FROM products p
                JOIN orderlines o
                    ON p.productid = o.productid''')

            price_range = [
                int(range_response.results[0][1]),
                int(range_response.results[0][0])
            ]
            range_step = int((price_range[1] - price_range[0]) / bins)
            current_step = price_range[0]

            for i in range(bins):
                start = current_step
                end = current_step + range_step
                bin_ranges.append((start, end))

                current_step = end

        for bin in bin_ranges:
            start = bin[0]
            end = bin[1]

            col_name = 'count_' + str(start) + '_to_' + str(end)

            bin_selects.append('''
                SUM(
                    CASE
                        WHEN
                            o.unitprice::Numeric >= {}
                            AND o.unitprice::Numeric < {}
                        THEN 1
                        ELSE 0
                    END
                ) as {}
                '''.format(start, end, col_name))

        return ', '.join(bin_selects)

    @log
    def priceDistribution(self, bins=5):
        bin_selects = self._createPriceBins(bins)
        query = '''
            SELECT {}
            FROM products p
            JOIN orderlines o
                ON p.productid = o.productid'''.format(bin_selects)

        return self._execSqlQuery(query)

    @log
    def seasonalOrderDistribution(self, asin=[]):
        asin_filter = ' WHERE p.asin IN %(asin_list)s ' if len(asin) > 0 else ' '

        query = ('''
            SELECT
                p.asin,
                p.productid,
                SUM(
                    CASE
                      WHEN
                        DATE_PART('month',o.billdate) >= 3
                        AND DATE_PART('month',o.billdate) <= 5
                      THEN 1
                      ELSE 0
                    END) as spring_sales,
                SUM(
                    CASE
                      WHEN
                        DATE_PART('month',o.billdate) >= 6
                        AND DATE_PART('month',o.billdate) <= 8
                      THEN 1
                      ELSE 0
                    END) as summer_sales,
                SUM(
                    CASE
                      WHEN
                        DATE_PART('month',o.billdate) >= 9
                        AND DATE_PART('month',o.billdate) <= 11
                      THEN 1
                      ELSE 0
                    END) as fall_sales,
                SUM(
                    CASE
                      WHEN
                        DATE_PART('month',o.billdate) >= 12
                        OR DATE_PART('month',o.billdate) <= 2
                      THEN 1
                      ELSE 0
                    END) as winter_sales
              FROM products p
              JOIN orderlines o
                ON p.productid = o.productid '''
                 + asin_filter + '''
              GROUP BY p.asin, p.productid''')

        return self._execSqlQuery(query,
                                  {
                                      'asin_list': tuple(asin)
                                  })

    @log
    def ratingsDistribution(self, min_date='1900-1-1', max_date=None, asin=[], sample_size=100):
        """For each product, determine the how many 1, 2, 3, 4, and 5 star reviews the product received. 

        Args:
            min_date (string): optional. date. inclusive bottom limit of reviewTime
            max_date (string): optional. date. inclusive upper limit of reviewTime
            asin (list):optional. The asins of the products the rating distrubtion will be produced
            for. Defaults to returning distributions for all asins
            sample_size (int): optional. Percentage of the reviews the query will run over.
        
        Returns:
            QueryResponse(asin, productid, 'one_star_votes', 'two_star_votes', 'three_star_votes',
             'four_star_votes', 'five_star_votes) (tuple(str,int,int,int,int,int,int)):
             asin is the label for the book. productid is the unique identifier for the product.
             one_star_votes is the number of one star reveiws the book
             received. two_star_votes is the number of two star reveiws the book received. 
             three_star_votes is the number of three star reveiws the book received. four_star_votes is
             the number of four star reveiws the book received. five_star_votes is the number of five
             star reveiws the book received. """

        max_date_filter = ' AND r.ReviewTime <= %(max_date)s' if max_date else ' '

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
                                      'min_date': min_date,
                                      'max_date': max_date,
                                      'asin_list': tuple(asin),
                                      'sample_size': sample_size,
                                      'random_seed': self._random_seed
                                  })

    @log
    def coPurchases(self, asin, min_date='1900-1-1', max_date=None, sample_size=100):
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
              AND o.orderdate >= %(min_date)s''' + max_date_filter + '''
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
        return self._execSqlQuery(query,
                                  {
                                      'min_date': min_date,
                                      'max_date': max_date,
                                      'asin_list': tuple(asin),
                                      'sample_size': sample_size,
                                      'random_seed': self._random_seed
                                  })

    @log
    def statsByProduct(self,asin=[], min_date='1900-1-1', max_date=None, sample_size=100):
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
        asin_filter = (' AND products.asin in %(asin_list)s '
                            if (asin is not None) & (len(asin) > 0)
                            else ' ')
        
        query = ('''
                  SELECT orderlines.productid,
                      products.asin,
                      count(*) as numOrders,
                      avg(r.overall) as avgrating,
                      products.nodeid as category,
                      DATE_PART('day', max(shipdate)::TIMESTAMP - min(shipdate)::TIMESTAMP) as
                      days_on_sale
                  FROM orderlines
                      JOIN products
                        ON orderlines.productid = products.productid
                      JOIN orders o
                        ON orderlines.orderid = o.orderid
                      JOIN reviews r
                        ON products.asin=r.asin
                      JOIN calendar
                        ON o.orderdate=calendar.date
                  WHERE orderlines.numunits > 0 '''
                        + asin_filter
                        + '''
                  group by orderlines.productid, products.asin, products.nodeid;''')
        return self._execSqlQuery(query,
               {
                    'min_date':min_date,
                    'max_date':max_date,
                    'asin_list':tuple(asin),
                    'sample_size':sample_size,
                    'random_seed':self._random_seed
               })

    @log
    def byCategory(self, nodeid):
        nodeid_filter = (' WHERE nodeid in %(nodeid)s '
            if type(nodeid) is list
            else ' WHERE nodeid = %(nodeid)s ')
        return self._execSqlQuery('''
            SELECT productid
            FROM products'''
            +nodeid_filter, {'nodeid':tuple(nodeid) if type(nodeid) is list else str(nodeid)})

    @log
    def clusterProducts(self,
                        feature_set=None,
                        n_clusters=8,
                        algorithm='auto',
                        cluster_on=[
                            'numorders',
                            'avgrating',
                            'category',
                            'days_on_sale',
                            'spring_sales',
                            'summer_sales',
                            'fall_sales',
                            'winter_sales',
                            'one_star_votes',
                            'two_star_votes',
                            'three_star_votes',
                            'four_star_votes',
                            'five_star_votes',
                        ],
                        random_state=None,
                        asin=None,
                        scale=False,
                        n_components=13):
        """Clusters the books together using KMeans clustering utilizing the clusterQuery 
        results as the features (num_orders, avgrating, category, and days_on_sale).

        Args: 
            num_clusters (int): optional. default=8 The number of clusters to form as well as 
            the number of centroids to generate.
            algorithm (string): optional. “auto”, “full” or “elkan”, default=”auto”. K-means algorithm 
            to use. The classical EM-style algorithm is “full”. The “elkan” variation is more efficient
            by using the triangle inequality, but currently doesn’t support sparse data. “auto” chooses
            “elkan” for dense data and “full” for sparse data.
            random_state (int): optional. int used to genderate random number.
            asin (tuple(string)): optional. asins will be the centers of the kmeans clustering.
            n_components (int): optional. default =13. will be used to determine if PCA will be used. 
            If n_components < len(cluster_on), PCA will be used to reduce the dim to n_components
        
        Returns:
             tuple(productid, asin, y_pred): productid is the products unqiue identifier. asin is the
             identification of the book. y_pred is the label of the clsuter that the product belongs to.
        """
        if feature_set is None:
            feature_set = self.statsByProduct()
        if (not hasattr(feature_set, 'results')) | (not hasattr(feature_set, 'columns')):
            raise Exception('invalid feature set, no results or columns')

        data=pd.DataFrame(feature_set.results, columns=feature_set.columns)
        data_asin=data['asin'].values
        response=self.seasonalOrderDistribution(asin=data_asin)
        df=pd.DataFrame(response.results,columns=response.columns)
        data=data.merge(df, on=['productid','asin'],how='outer')
        ratings=self.ratingsDistribution(asin=data_asin)
        df=pd.DataFrame(ratings.results,columns=ratings.columns)
        data=data.merge(df, on=['productid','asin'],how='outer')
        

        if (data[cluster_on].isnull().values.any()):
            data = data.dropna()

        if asin is None:
            input_centers = 'k-means++'
            X = (
                data[cluster_on].values
                if scale is False
                else StandardScaler().fit_transform(data[cluster_on].values)
            )

        else:

            asin_s = set(asin)
            input_centers = data[data['asin'].isin(asin)]
            input_centers = input_centers[cluster_on].values

            data = data[~data['asin'].isin(asin)]
            X = (
                data[cluster_on].values
                if scale is False
                else StandardScaler().fit_transform(data[cluster_on].values)
            )

        if n_components < len(cluster_on):
            pca=PCA(n_components=n_components)
            X=pca.fit_transform(X)
            
            clustering=pd.DataFrame(X, columns=range(n_components))
        
        else:
            clustering = pd.DataFrame(X, columns=cluster_on)

        algorithm=KMeans(n_clusters=(n_clusters), algorithm=(algorithm),init=input_centers, 
                         random_state=random_state)
        algorithm.fit_predict(X)

        for col in data.columns:
            if col not in clustering.columns.values:
                clustering.loc[:, col] = data[col]

        response_columns = np.append(clustering.columns.values, 'cluster')
        clustering.loc[:, 'cluster'] = algorithm.labels_

        response = QueryResponse(
            columns=response_columns,
            results=[tuple(x) for x in clustering.values]
        )

        return response
