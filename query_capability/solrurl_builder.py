import re
from datalog_parser import DatalogParser
from category import Category

__all__ = ['SQLPPBuilder']

class SolrUrlBuilder:
    def __init__(self, q):
        self.parser = DatalogParser(q)

    def getQueryUrlParams(self, special_handler=None):
        str_params = ''
        return str_params

