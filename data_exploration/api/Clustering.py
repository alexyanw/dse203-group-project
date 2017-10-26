#this probably does not work yet

from util.sql_source import SqlSource
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA

class Clustering(SqlSource):

    def Zip_CrosstabByProductid(self):
        df = pd.read_sql('''select o.zipcode, ol.productid, ol.orderlineid  
                            from orders o, orderlines ol  
                            where o.orderid = ol.orderid''', con=conn)
        zipcode = np.array(df['zipcode'])
        productid = np.array(df['productid'])
        return pd.crosstab(zipcode, [productid])


    def Zip_PCA(self, crosstab = None, n_components=10):
        pca = PCA(n_components=n_components)
        data_transformed = pca.fit_transform(matrix[matrix.columns.difference(['row_0'])] )
        return pd.DataFrame(data_transformed)
    
    def Zip_KMeans(self, n_clusters=10, random_state=0):
        kmeans = KMeans(n_clusters=n_clusters, random_state=random_state).fit(data_transformed)
        clusters = kmeans.labels_
        clusters_df = pd.DataFrame(clusters)
        clusters_df['labels'] = zips
        return clusters_df