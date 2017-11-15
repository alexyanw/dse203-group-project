from urllib import parse, request
from json import loads
from review_text import ReviewText

__all__ = ['SolrEngine']

class QueryResponse:
    def __init__(self, raw_response):
        self._json = loads(raw_response)

        self.requestID = self._json['requestID'] if 'requestID' in self._json else None
        self.clientContextID = self._json['clientContextID'] if 'clientContextID' in self._json else None
        self.signature = self._json['signature'] if 'signature' in self._json else None
        self.results = self._json['results'] if 'results' in self. _json else None
        self.metrics = self._json['metrics'] if 'metrics' in self._json else None

class SolrEngine:
    def __init__(self, server = 'localhost', port = 8983):
        self._server = server
        self._port = port
        core_name = "bookstore"
        self.baseurl = "http://" + server + ":" + str(port) + "/solr/" + core_name 

        self.schema_wrapper = {
            'review_text': ReviewText,
        }
        self.special_handler = {
        }


    def execute(self, params, **kwargs):
        #"/select?q=" + query
        queryurl = self.baseurl += "/select?" + params
        if 'debug' in kwargs: return queryurl

        req = request.Request(self.baseurl)
        result = request.urlopen(req)
        #decode = 'iso-8859-1'
        #str_data = result.read().decode(decode)
        str_data = result.read()
        return QueryResponse(str_data)

    def queryDatalog(self, datalog, **kwargs):
        builder = SolrUrlBuilder(datalog)
        params = builder.getQueryCmd(self.special_handler)

        return self.execute(params, **kwargs)

