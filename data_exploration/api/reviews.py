from datasources import SqlSource, SolrSource
from logger import log

class Reviews(SqlSource, SolrSource):

    def __init__(self, sql_config, solr_config):
        SqlSource.__init__(self, sql_config)
        SolrSource.__init__(self, solr_config)

    @log
    def termsByAsin(self, asin):
        return self._execSolrQuery(
            'asin:"{}"'.format(asin),
            {
                'facet':'true',
                'facet.field':'reviewText'
            })

    @log
    def asinByTerms(self, terms):
        return self._execSolrQuery(
            'reviewText:"{}"'.format(','.join(terms)),
            **{
                'facet': 'true',
                'facet.field': 'asin'
            })