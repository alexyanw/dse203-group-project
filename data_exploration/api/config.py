import json

class ConfigType(object):
    def __init__(self, d):
        self._config = d # set it to conf

    def get_property(self, property_name):
        if property_name not in self._config.keys(): # we don't want KeyError
            return None  # just return None if not found
        return self._config[property_name]

class SqlConfig(ConfigType):
    @property
    def connection_string(self):
        return self.get_property('connectionString')

    @property
    def random_seed(self):
        return self.get_property('randomSeed')

    @property
    def cache_ttl(self):
        return self.get_property('cacheTtl')

class AsterixConfig(ConfigType):
    @property
    def host(self):
        return self.get_property('host')

    @property
    def collection(self):
        return self.get_property('collection')

    @property
    def cache_ttl(self):
        return self.get_property('cacheTtl')

class SolrConfig(ConfigType):
    @property
    def host(self):
        return self.get_property('host')

    @property
    def cache_ttl(self):
        return self.get_property('cacheTtl')

class Config(object):
    def __init__(self, path='.config'):
        with open(path) as config_file:
            config = json.load(config_file)

        self.sql_config = SqlConfig(config['sqlConfig'])
        self.asterix_config = AsterixConfig(config['asterixConfig'])
        self.solr_config = SolrConfig(config['solrConfig'])

