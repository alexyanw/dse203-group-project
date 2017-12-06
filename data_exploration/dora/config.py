import json
import os

class ConfigType(object):
    """Wrapper type for config property accessibility."""
    def __init__(self, d):
        self._config = d

    def get_property(self, property_name):
        if property_name not in self._config.keys():
            return None
        return self._config[property_name]

class SqlConfig(ConfigType):
    @property
    def connection_string(self):
        """SQL Connection string. Should be compatible with psycopg2."""
        return self.get_property('connectionString')

    @property
    def random_seed(self):
        """Integer for query sampling. Using the same seed will result in repeatable queries."""
        return self.get_property('randomSeed')

    @property
    def cache_ttl(self):
        """Cache time-to-live in seconds"""
        return self.get_property('cacheTtl')

class AsterixConfig(ConfigType):
    @property
    def host(self):
        """ AsterixDB host API url.

        ie, http://localhost:19002"""
        return self.get_property('host')

    @property
    def collection(self):
        """Collection to query"""
        return self.get_property('collection')

    @property
    def cache_ttl(self):
        """Cache time-to-live in seconds"""
        return self.get_property('cacheTtl')

class SolrConfig(ConfigType):
    @property
    def host(self):
        """Solr host API url.

        ie, http://localhost:8983/solr/bookstore_pr/"""
        return self.get_property('host')

    @property
    def cache_ttl(self):
        """Cache time-to-live in seconds"""
        return self.get_property('cacheTtl')

class Config(object):

    def __init__(self, path=None):
        """Data Explorer config object.

        Args:
                path (str): path to a properly formatted json file. defaults to default.config in package dir.
            """
        if (path is None):
            path = os.path.join(os.path.dirname(__file__), 'default.config')
        elif (type(path) is str) & (not os.path.exists(path)):
            path = os.path.join(os.path.dirname(__file__), 'default.config')

        with open(path) as config_file:
            config = json.load(config_file)

        self.sql_config = SqlConfig(config['sqlConfig'])
        self.asterix_config = AsterixConfig(config['asterixConfig'])
        self.solr_config = SolrConfig(config['solrConfig'])

