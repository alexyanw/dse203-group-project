import re
import pandas as pd
from postgres_engine import PostgresEngine
from asterix_engine import AsterixEngine
from solr_engine import SolrEngine
from datalog_parser import DatalogParser
from combiner import Combiner

__all__ = ['HybridEngine']

class HybridEngine:
    def __init__(self, **kwargs):
        self.engines = {}
        self.engines['postgres'] = PostgresEngine(kwargs['postgres']) if 'postgres' in kwargs else PostgresEngine()
        self.engines['asterix'] = AsterixEngine(kwargs['asterix']) if 'asterix' in kwargs else AsterixEngine()
        self.engines['solr'] = SolrEngine(kwargs['solr']) if 'solr' in kwargs else SolrEngine()
        self.mode = None

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
        elif view in sources and len(sources) == 2:
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

    def resolveJoinPath(self, join_columns, source_tables):
        join_paths = {}
        join_paths['MULTI_SOURCE'] = {}
        for col,tables in join_columns.items():
            col_source = {}
            for table in tables:
                source = [s for s,ts in source_tables.items() if table in ts][0]
                if source not in col_source: col_source[source] = []
                col_source[source].append(source+'.'+table)
            if len(col_source) == 1:
                source = list(col_source.keys())[0]
                if source not in join_paths: join_paths[source] = {}
                join_paths[source][col] = tables
            else:
                join_paths['MULTI_SOURCE'][col] = [table for tables in col_source.values() for table in tables]
        return join_paths

    def resolveReturnColumns(self, parser, join_path):
        return_columns = {'MULTI_SOURCE': []}
        for source, tables in parser.source_tables.items():
            columns = []
            table_columns = []
            for col in parser.return_columns:
                if col in parser.column_to_table:
                    table = parser.column_to_table[col].get(source, None)
                    if not table: continue
                    columns.append(col)
                    table_columns.append({'table':table,'column':col,'func':None,'alias':None})
                    return_columns['MULTI_SOURCE'].append({'table':table,'column':col,'func':None,'alias':None})
                elif col in parser.aggregation:
                    func,src_col = parser.aggregation[col]
                    table = parser.column_to_table[src_col].get(source, None)
                    if not table: continue
                    if parser.single_source():
                        table_columns.append({'table':table,'column':src_col,'func':func,'alias':col})
                    else:
                        table_columns.append({'table':table,'column':src_col,'func':None,'alias':None})
                        return_columns['MULTI_SOURCE'].append({'table':table,'column':src_col,'func':func,'alias':col})

            for col in join_path['MULTI_SOURCE']:
                table = parser.column_to_table[col].get(source, None)
                if table and col not in columns:
                    table_columns.append({'table':table,'column':col,'func':None,'alias':None})
            return_columns[source] = table_columns
        return return_columns

    def arbiter(self, parser, **kwargs):
        #join_path = self.resolveJoinPath(parser.join_columns, parser.source_tables)
        #return_columns = self.resolveReturnColumns(parser, join_path)
        if 'debug' in kwargs: 
            print("###### return columns #######")
            print(parser.return_columns, "\n")

            print("###### join path #######")
            print(parser.join_path, "\n")

        results = {}
        for source, tables in parser.source_tables.items():
            subquery = {
                'return': parser.return_columns[source],
                'tables': tables,
                'columns': parser.table_columns,
                'column_idx': {t: parser.table_column_idx[t] for t in tables},
                'conditions': parser.table_conditions.get(source,None),
                'join': parser.join_path.get(source, None),
                'groupby': parser.groupby if parser.single_source() else None,
                'limit': parser.limit if parser.single_source() else None,
                }
            results[source] = self.engines[source].queryDatalog(subquery, **kwargs)
        return results

    def debugPrint(self, sub_results):
        for source in sub_results:
            print("###### query for", source, "#######")
            print(sub_results[source], "\n")

