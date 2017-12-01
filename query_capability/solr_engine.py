import re
from urllib import parse, request
import pandas as pd
import pysolr
from json import loads
from solrurl_builder import SolrUrlBuilder
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
        self.baseurl = "http://" + self._server + ":" + str(self._port) + "/solr/" + self._core + "/"
        self._solr_conn = solr = pysolr.Solr(self.baseurl, search_handler="/tvrh", timeout=10)

        self.schema_wrapper = {
            'review_text': ReviewText,
        }

    def execute(self, params, **kwargs):
        results = self._solr_conn.search('*:*', **params)
        return results

    def queryDatalog(self, datalog, **kwargs):
        if len(datalog['tables']) != 1 or datalog['tables'][0] != 'review_text':
            print("Error: datalog query to solr must be on table review_text")
            exit(1)
        table = datalog['tables'][0]
        wrapper_class = self.schema_wrapper[table]
        builder = SolrUrlBuilder(datalog, self.schema_wrapper)
        requests = builder.getQueryRequests(**kwargs)
        df_results = None
        for req in requests:
            solr_results = self.execute(req['params'], **kwargs)
            result = wrapper_class().resolve(solr_results, table, req['features'])
            if df_results is None: df_results = result
            else: df_results = df_results.merge(result, on='id')

        df_results = self.filter_by_condition(df_results, datalog['conditions'][table])
        # filter result by return columns
        ret_cols = [ret['column'] for ret in datalog['return']]
        return df_results[ret_cols]

    def filter_by_condition(self, df_in, conditions):
        df_results = df_in
        for cond in conditions:
            col,op,value = cond
            if re.search('^\d+$', value): value = int(value)
            if op == '>': df_results = df_results[df_results[col] > value]
            elif op == '>=': df_results = df_results[df_results[col] >= value]
            elif op == '<':  df_results = df_results[df_results[col] < value]
            elif op == '<=': df_results = df_results[df_results[col] <= value]
            elif op == '=': df_results = df_results[df_results[col] == value]
            elif op == '!=': df_results = df_results[df_results[col] == value]
            
        return df_results
