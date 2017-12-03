import re
import pandas as pd
from postgres_engine import PostgresEngine
from asterix_engine import AsterixEngine
from solr_engine import SolrEngine
from datalog_parser import DatalogParser
from combiner import Combiner
from writeback import Writeback

__all__ = ['HybridEngine']

class HybridEngine:
    def __init__(self, **kwargs):
        self.engines = {}
        self.engines['postgres'] = PostgresEngine(kwargs['postgres']) if 'postgres' in kwargs else PostgresEngine()
        self.engines['asterix'] = AsterixEngine(kwargs['asterix']) if 'asterix' in kwargs else AsterixEngine()
        self.engines['solr'] = SolrEngine(kwargs['solr']) if 'solr' in kwargs else SolrEngine()
        self.mode = None
        self.writeback = Writeback(self.engines['postgres'])

    def analyze(self, datalog, **kwargs):
        #if type(datalog) is dict: return self.querySubDatalog(datalog)
        if type(datalog) is not list:
            print("Error: datalog must be list or dict")
            exit(1)

        self.parsers = [DatalogParser(d) for d in datalog]
        sources = []
        for p in self.parsers:
            sources += list(p.source_tables.keys())
        sources = set(sources)

        if len(datalog) == 1:
            self.mode = 'single'
        elif 'view' in sources and len(sources) == 2:
            self.mode = 'single_view'
        elif len(set([p.return_table for p in self.parsers])) == 1:
            self.mode = 'union'
        elif len(self.parsers) == 2:
            self.mode = 'view'
        else: # sub datalogs > 2
            print("Not support multi views")
            exit(1)

    def queryDatalog(self, datalog, **kwargs):
        self.analyze(datalog, **kwargs)

        source_results = []
        if self.mode in ['single', 'union']:
            source_results = [self.querySubDatalog(p, **kwargs) for p in self.parsers]
        elif self.mode == 'single_view':
            exit(1)
        elif self.mode in ['view']:
            source_results = [self.querySubDatalog(p, **kwargs) for p in self.parsers[:1]]

        combiner = Combiner(self.mode, source_results, self.parsers)
        return combiner.process(**kwargs)

    def querySubDatalog(self, parser, **kwargs):
        source_result = self.arbiter(parser, **kwargs)
        if 'debug' in kwargs:
            self.debugPrint(source_results)
            return None
        return source_result

    def arbiter(self, parser, **kwargs):
        if 'debug' in kwargs: 
            print("###### query columns #######")
            print(parser.query_columns, "\n")

            print("###### join path #######")
            print(parser.join_path, "\n")

        results = {}
        for source, tables in parser.source_tables.items():
            subquery = {
                'return': parser.query_columns[source],
                'tables': tables,
                'columns': parser.table_columns,
                'column_idx': {t: parser.table_column_idx[t] for t in tables},
                'conditions': parser.table_conditions.get(source,None),
                'join': parser.join_path.get(source, None),
                'groupby': parser.groupby if parser.single_source() else None,
                'orderby': parser.orderby if parser.single_source() else None,
                'limit': parser.limit if parser.single_source() else None,
                }
            results[source] = self.engines[source].query(subquery, **kwargs)
        return results

    def create(self, table):
        self.writeback.create(table)

    def write(self, table, df):
        self.writeback.write(table, df)

    def execute(self, sqlcmd):
        self.engines['postgres'].execute(sqlcmd)

    def debugPrint(self, sub_results):
        for source in sub_results:
            print("###### query for", source, "#######")
            print(sub_results[source], "\n")

