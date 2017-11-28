from urllib import parse, request
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
        self._server = cfg.get('server', 'localhost')
        self._port = cfg.get('port', 8983)
        self._database = cfg.get('database', 'bookstore')
        core_name = "bookstore"
        self.baseurl = "http://" + self._server + ":" + str(self._port) + "/solr/" + self._database

        self.schema_wrapper = {
            'review_text': ReviewText,
        }
        self.special_handler = {
        }

    def execute(self, params, **kwargs):
        #"/select?q=" + query
        queryurl = self.baseurl + "/select?" + params
        if 'debug' in kwargs: return queryurl

        req = request.Request(self.baseurl)
        result = request.urlopen(req)
        #decode = 'iso-8859-1'
        #str_data = result.read().decode(decode)
        str_data = result.read()
        return SolrResponse(str_data)

    def queryDatalog(self, datalog, **kwargs):
        builder = SolrUrlBuilder(datalog)
        params = builder.getQueryUrl(self.special_handler)

        return self.execute(params, **kwargs)

