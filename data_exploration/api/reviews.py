from util.sql_source import SqlSource
from util.solr_source import SolrSource
from datetime import datetime
from nltk.corpus import stopwords

class Reviews(SqlSource, SolrSource):

    def __init__(self, sql_connection_str, solr_server, random_seed):
        SqlSource.__init__(self, sql_connection_str, random_seed)
        SolrSource.__init__(self, solr_server)

    def termsByAsin(self, asin):
        return self._execSolrQuery(
            'asin:"{}"'.format(asin),
            **{
                'facet':'true',
                'facet.field':'reviewText'
            })



    def asinByTerms(self, terms):
        return self._execSolrQuery(
            'reviewText:"{}"'.format(terms),
            **{
                'facet': 'true',
                'facet.field': 'asin'
            })