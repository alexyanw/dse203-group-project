import re
from datalog_parser import DatalogParser
import pprint

__all__ = ['SolrUrlBuilder']

class SolrUrlBuilder:
    def __init__(self, q, schema=None):
        self.datalog = q
        self.schema = schema

    def getColumnName(self, table, col_or_val, full=True):
        if not self.schema:
            return col_or_val
        if col_or_val not in self.datalog['column_idx'][table]:
            return col_or_val
        col_idx = self.datalog['column_idx'][table][col_or_val]
        if full:
            return table + '.' + self.schema[table].getColumn(table, col_idx)
        else:
            return self.schema[table].getColumn(table, col_idx)

    def getQueryRequests(self, **kwargs):
        requests = []
        for table in self.datalog['columns']:
            if table not in self.schema: continue
            request_features = {}
            columns = self.datalog['columns'][table]
            for dst_col in columns:
                if dst_col == '_': continue
                src_col = self.getColumnName(table, dst_col, False)
                request_features[src_col] = dst_col
            request = {
                'params': self.getQueryParams(request_features.values()),
                'features': request_features,
            }
            # NOTE: better to check just return and conditions for query feature list
            #'conditions': {'review_text': [['review_length', '>', '100']]}
            #for struct in self.datalog['conditions']:
            #    struct.values()[1]
            requests.append(request)

        return requests

    def getQueryParams(self, feature_list):
        #http://132.249.238.28:8983/solr/bookstore_pr/tvrh?q=*%20TO%20*&fl=id,asin,reviewText&rows=2&indent=true&tv=true&tv.tf=true&tv.df=true&tv.fl=reviewText
        params = {
            'indent': 'true',
            'fl': 'id,asin,reviewerID,reviewText',
            'tv': 'true',
            'tv.fl': 'reviewText',
            'tv.tf': 'true',
            'tv.df': 'true',
            'tv.tf_idf': 'true',
        }
        limit = self.datalog.get('limit', None)
        if limit:
            params['rows'] = limit

        return params

