from urllib import parse, request
import pysolr
from json import loads
from review_text import ReviewText

__all__ = ['SolrEngine']

class SolrResponse:
    def __init__(self, raw_response):
        self._json = loads(raw_response)

        self.requestID = self._json['requestID'] if 'requestID' in self._json else None
        self.clientContextID = self._json['clientContextID'] if 'clientContextID' in self._json else None
        self.signature = self._json['signature'] if 'signature' in self._json else None
        self.results = self._json['results'] if 'results' in self. _json else None
        self.metrics = self._json['metrics'] if 'metrics' in self._json else None

class SolrEngine:
    def __init__(self, cfg={}):
        self._server = cfg.get('server', '132.249.238.28')
        self._port = cfg.get('port', 8983)
        self._core = cfg.get('core', 'bookstore_pr')
        self.baseurl = "http://" + self._server + ":" + str(self._port) + "/solr/"
        #http://localhost:8983/solr/
        self._solr_conn = solr = pysolr.Solr(self.baseurl, timeout=10)

        self.schema_wrapper = {
            'review_text': ReviewText,
        }
        self.special_handler = {
        }

    def execute(self, params, **kwargs):
        results = solr.search(self._core, params)
        #return SolrResponse(str_data)
        return results

    def queryDatalog(self, datalog, **kwargs):
        builder = SolrUrlBuilder(datalog)
        params = builder.getQueryUrl(self.special_handler, **kwargs)
        return self.execute(params, **kwargs)

