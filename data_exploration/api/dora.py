from orders import *
from products import *
from reviews import *

class DataExplorer:
    def __init__(self,
                 sql_connection_str="dbname='sqlbook' user='postgres' host='localhost' password=''",
                 asterix_url='http://localhost',
                 solr_url='http://localhost:8983/solr/bookstore/',
                 random_seed=203):

        self.orders = Orders(sql_connection_str, random_seed)
        self.products = Products(sql_connection_str, random_seed)
        self.reviews = Reviews(sql_connection_str, solr_url, random_seed)

if __name__ == '__main__':
    explorer = DataExplorer()
    stats = explorer.orders.statsByZipcode()
    print(stats.columns)
    #print(stats.results)

    dist = explorer.products.ratingsDistribution(asin=('0007386648','0002007770'))
    print(dist.results)

    terms = explorer.reviews.termsByAsin(asin='0007386648')
    print(terms.docs)
    print(terms.facets)
