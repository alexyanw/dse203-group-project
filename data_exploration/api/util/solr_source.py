import pysolr


class SolrSource:
    def __init__(self, server_url):
        self._solr_conn = solr = pysolr.Solr(server_url, timeout=10)

    def _execSolrQuery(self, q, **kwargs):
        return self._solr_conn.search(
            q=q,
            search_handler='select',
            **kwargs)