from datasources import SqlSource, SolrSource
from datetime import datetime
from nltk.corpus import stopwords

class Reviews(SqlSource, SolrSource):

    def __init__(self, sql_config, solr_config):
        SqlSource.__init__(self, sql_config)
        SolrSource.__init__(self, solr_config)

    def termsByAsin(self, asin):
        return self._execSolrQuery(
            'asin:"{}"'.format(','.join(asin)),
            **{
                'facet':'true',
                'facet.field':'reviewText'
            })



    def asinByTerms(self, terms):
        return self._execSolrQuery(
            'reviewText:"{}"'.format(','.join(terms)),
            **{
                'facet': 'true',
                'facet.field': 'asin'
            })