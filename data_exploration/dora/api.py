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
        self._products = Products(config.sql_config)
        self._reviews = Reviews(config.sql_config, config.solr_config)
        self._orders = Orders(config.sql_config)
        self._categories = Categories(config.asterix_config)
        self._customers = Customers(config.sql_config)
        self._benchmarks = Benchmarks(config.sql_config)
        self._recommendations = Recommendations(config.sql_config)

    @property
    def products(self):
        """SqlSource: submodule to query products"""
        return self._products

    @property
    def reviews(self):
        """SqlSource, SolrSource: submodule to query reviews"""
        return self._reviews

    @property
    def orders(self):
        """SqlSource: submodule to query orders"""
        return self._orders

    @property
    def categories(self):
        """AsterixSource: submodule to query categories"""
        return self._categories

    @property
    def customers(self):
        """SqlSource: submodule to query customers"""
        return self._customers

    @property
    def benchmarks(self):
        """SqlSource: submodule to query api benchmarks"""
        return self._benchmarks

    @property
    def recommendations(self):
        """SqlSource: submodule to query ML model output"""
        return self._recommendations