import pandas as pd
from urllib import parse, request
from json import loads
from sqlpp_builder import SQLPPBuilder
from category import Category
from source_schema import SourceTable

__all__ = ['AsterixEngine']

class QueryResponse:
    def __init__(self, raw_response):
        self._json = loads(raw_response)

        self.requestID = self._json['requestID'] if 'requestID' in self._json else None
        self.clientContextID = self._json['clientContextID'] if 'clientContextID' in self._json else None
        self.signature = self._json['signature'] if 'signature' in self._json else None
        self.results = self._json['results'] if 'results' in self. _json else None
        self.metrics = self._json['metrics'] if 'metrics' in self._json else None

class AsterixEngine:
    def __init__(self, cfg={}):
        self._server = cfg.get('server', 'http://localhost')
        self._port = cfg.get('port', 19002)
        self.dburl = self._server + ':' + str(self._port) + '/query/service'
        self.schema_wrapper = {
            'CategoryLevel': Category,
            'CategoryFlat': Category,
            'ClassificationInfo': SourceTable,
            #'Reviews': Reviews,
        }
        self.special_handler = {
            'CategoryFlat.category': Category.handleCategoryFlatCategory,
        }

    def execute(self, statement, **kwargs):
        if 'debug' in kwargs: return statement
        payload = {
            'statement': statement,
        }
        if 'pretty' in kwargs:
            payload['pretty'] = kwargs['pretty']
        if 'client_context_id' in kwargs:
            payload['client_context_id'] = kwargs['client_context_id']

        req = request.Request(self.dburl, parse.urlencode(payload).encode("utf-8"))
        resource = request.urlopen(req)
        #decode = resource.headers.get_content_charset()
        decode = 'iso-8859-1'
        str_data = resource.read().decode(decode)
        return QueryResponse(str_data).results

    def queryDatalog(self, datalog, **kwargs):
        builder = SQLPPBuilder(datalog, self.schema_wrapper)
        views = []
        for table in datalog['tables']:
            if self.schema_wrapper[table] == SourceTable: continue
            wrapper_class = self.schema_wrapper[table]
            views.append(wrapper_class().get_view(table, view=True,**kwargs))
        sqlppcmd = builder.getQueryCmd(self.special_handler)

        if views:
            sqlppcmd = "use TinySocial;\nWITH {}\n{}".format(",\n".join(views), sqlppcmd)

        results = self.execute(sqlppcmd, **kwargs)
        if 'debug' in kwargs: return results
        return pd.DataFrame(results)

