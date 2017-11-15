from orders import *
from products import *
from reviews import *
from config import Config

class DataExplorer:
    def __init__(self,config = Config()):
        self.products = Products(config.sql_config)
        self.reviews = Reviews(config.sql_config, config.solr_config)
        self.orders = Orders(config.sql_config)


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

    dist2 = explorer.products.ratingsDistribution(asin=('0007386648', '0002007770'))
    print(dist2.results)
