from .orders import Orders
from .products import Products
from .reviews import Reviews
from .categories import Categories
from .customers import Customers
from .benchmarks import Benchmarks
from .recommendations import Recommendations

from .config import Config


class DataExplorer:
    def __init__(self, config=Config()):
        self.products = Products(config.sql_config)
        self.reviews = Reviews(config.sql_config, config.solr_config)
        self.orders = Orders(config.sql_config)
        self.categories = Categories(config.asterix_config)
        self.customers = Customers(config.sql_config)
        self.benchmarks = Benchmarks(config.sql_config)
        self.recommendations = Recommendations(config.sql_config)
