from .orders import Orders
from .products import Products
from .reviews import Reviews
from .categories import Categories
from .customers import Customers
from .benchmarks import Benchmarks
from .recommendations import Recommendations

from .config import Config


class DataExplorer:
    """Main instantiated class for Dora package

        Contains properties for each submodule in the package. Init
        with a Config object.
        """
    def __init__(self, config=Config()):
        self.products = Products(config.sql_config)
        self.reviews = Reviews(config.sql_config, config.solr_config)
        self.orders = Orders(config.sql_config)
        self.categories = Categories(config.asterix_config)
        self.customers = Customers(config.sql_config)
        self.benchmarks = Benchmarks(config.sql_config)
        self.recommendations = Recommendations(config.sql_config)

    @property
    def products(self):
        """SqlSource: submodule to query products"""
        return self.products

    @property
    def reviews(self):
        """SqlSource, SolrSource: submodule to query reviews"""
        return self.reviews

    @property
    def orders(self):
        """SqlSource: submodule to query orders"""
        return self.orders

    @property
    def categories(self):
        """AsterixSource: submodule to query categories"""
        return self.categories

    @property
    def customers(self):
        """SqlSource: submodule to query customers"""
        return self.customers

    @property
    def benchmarks(self):
        """SqlSource: submodule to query api benchmarks"""
        return self.benchmarks

    @property
    def recommendations(self):
        """SqlSource: submodule to query ML model output"""
        return self.recommendations