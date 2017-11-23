import re
from datalog_parser import DatalogParser

__all__ = ['SolrUrlBuilder']

class SolrUrlBuilder:
    def __init__(self, q):
        self.parser = DatalogParser(q)

    def getQueryUrlParams(self, special_handler=None):
        str_params = ''
        return str_params

