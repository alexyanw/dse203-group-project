from orders import *
from products import *

class DoraDataExplorer:
    def __init__(self,
                 sql_connection_str="dbname='sqlbook' user='postgres' host='localhost' password='bosco241'",
                 asterix_url='http://localhost',
                 solr_url='',
                 random_seed=203):

        self.orders = Orders(sql_connection_str, random_seed)
        self.products = Products(sql_connection_str, random_seed)

if __name__ == '__main__':
    explorer = DoraDataExplorer()
    stats = explorer.orders.statsByZipcode()
    print(stats.columns)
    #print(stats.results)

    dist = explorer.products.ratingsDistribution()
    print(dist.results)