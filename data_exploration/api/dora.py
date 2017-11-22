from orders import *
from products import *
from reviews import *
from categories import *

from config import Config

class DataExplorer:
    def __init__(self,config = Config()):
        self.products = Products(config.sql_config)
        self.reviews = Reviews(config.sql_config, config.solr_config)
        self.orders = Orders(config.sql_config)
        self.categories = Categories(config.asterix_config)


if __name__ == '__main__':
    explorer = DataExplorer()
    #stats = explorer.orders.statsByZipcode()
    #print(stats.columns)
    #print(stats.results)

    # dist = explorer.products.ratingsDistribution(asin=['0007386648','0002007770'])
    # print(dist.results)
    #
    # terms = explorer.reviews.termsByAsin(asin=['0007386648'])
    # print(terms.columns)
    # print(terms.results)
    #
    #
    # dist2 = explorer.products.ratingsDistribution(asin=('0007386648', '0002007770'))
    # print(dist2.results)
    #
    # orders = explorer.orders.statsByZipcode()
    # print(orders.results)
    #
    # cat = explorer.categories.search('Architecture', classfication_only=True)
    # print(cat.results)
    #
    # cat = explorer.categories.search('Architecture', classfication_only=True)
    # print(cat.results)
    #
    # terms = explorer.reviews.termsByAsin(asin=['0007386648'])
    # print(terms.columns)
    # print(terms.results)

    cat = explorer.categories.parentsOf(173508)
    print(cat.results)
