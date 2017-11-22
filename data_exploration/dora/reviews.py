from .datasources import SqlSource, SolrSource
from .logger import log

class Reviews(SqlSource, SolrSource):

    def __init__(self, sql_config, solr_config):
        SqlSource.__init__(self, sql_config)
        SolrSource.__init__(self, solr_config)

    @log
    def termsByAsin(self, asin):
        """Given an ASIN, find the terms with the highest Solr score from its reviews.

        Solr score is configurable by facet.method parameter. Default 'fc' option used.
        https://lucene.apache.org/solr/guide/6_6/faceting.html#Faceting-Thefacet.methodParameter

        Args:
            asin (string): required. product identifier. Analyze terms for this product's reviews

        Returns:
             tuple(term, score): term is a token as defined by Solr. score is the metric Solr is configured for (see above).
        """
        return self._execSolrQuery(
            'asin:"{}"'.format(asin),
            {
                'facet':'true',
                'facet.field':'reviewText'
            })

    @log
    def asinByTerms(self, terms):
        """Given a list of words, find ASIN with the highest Solr score from its reviews.

        Solr score is configurable by facet.method parameter. Default 'fc' option used.
        https://lucene.apache.org/solr/guide/6_6/faceting.html#Faceting-Thefacet.methodParameter

        Args:
            terms (list): list of strings

        Returns:
           tuple(asin, score): asin is a product identifier. score is the metric Solr is configured for (see above).
        """

        return self._execSolrQuery(
            'reviewText:"{}"'.format(','.join(terms)),
            **{
                'facet': 'true',
                'facet.field': 'asin'
            })