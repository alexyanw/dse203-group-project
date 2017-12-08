import re
import logging, pprint
import pandas as pd
from datalog_parser_frontend import DatalogParserFE
from datalog_parser import DatalogParser
from postgres_engine import PostgresEngine
from asterix_engine import AsterixEngine
from solr_engine import SolrEngine
from combiner import Combiner
from writeback import Writeback
from source_schema import SourceTable
from utils import *

__all__ = ['HybridEngine']

logger = logging.getLogger('qe.HybridEngine')

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
            #fatal("single source view not supported yet")
        elif len(set([p.return_table for p in self.parsers])) == 1:
            self.mode = 'union'
        elif len(self.parsers) == 2:
            self.mode = 'view'
        else: # sub datalogs > 2
            fatal("Not support multi views")

    def queryDatalogRaw(self, datalog, **kwargs):
        fe = DatalogParserFE()
        subqueries = fe.parse(datalog)
        return self.queryDatalog(subqueries, **kwargs)

    def queryDatalog(self, datalog, **kwargs):
        if 'loglevel' in kwargs:
            set_logger(kwargs['loglevel'], kwargs.get('debug', False))
        logger.info("query datalog:\n{}\n".format(pprint.pformat(datalog)))
        self.analyze(datalog, **kwargs)

        source_results = []
        if self.mode in ['single', 'union']:
            source_results = [self.querySubDatalog(p, **kwargs) for p in self.parsers]
        elif self.mode == 'single_view':
            view_schema = {}
            for p in self.parsers[:-1]:
                source = 'postgres' if 'postgres' in p.query_columns else 'view'
                view_schema[p.return_table] = [col['column'] for col in p.query_columns[source]]
            source_result = None
            for p in self.parsers[:-1]:
                if not source_result:
                    source_result = self.querySubDatalog(p, returnview=p.return_table, **kwargs)
                else:
                    source_result = self.querySubDatalog(p, withview=source_result, viewschema=view_schema, returnview=p.return_table, **kwargs)
            source_results = [self.querySubDatalog(self.parsers[-1], withview=source_result, viewschema=view_schema, **kwargs)]
        elif self.mode == 'single_union':
            None
        elif self.mode in ['view']:
            source_results = [self.querySubDatalog(p, **kwargs) for p in self.parsers[:1]]

        if 'debug' in kwargs: return None
        combiner = Combiner(self.mode, source_results, self.parsers)
        return combiner.process(**kwargs)

    def querySubDatalog(self, parser, **kwargs):
        if parser.single_source_view():
            return self.querySubDatalogWithView(parser, **kwargs)

        results = {}
        for source, tables in parser.source_tables.items():
            view = kwargs.get('withview', None)
            source_engine = source
            if view: 
                source_engine = list(view.keys())[0]
                view = {'schema': kwargs['viewschema'], 'query': view[source_engine]}
            subquery = {
                'view': view,
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

            logger.debug("arbiter submit query to {}\n{}\n".format(source_engine, pprint.pformat(subquery)))
            results[source_engine] = self.arbiter(source_engine, subquery, **kwargs)

        return results

    def querySubDatalogWithView(self, parser, **kwargs):
        sources = set(parser.source_tables.keys()) - set(['view'])
        source_engine = list(sources)[0]
        results = {}
        view = kwargs.get('withview', None)
        if view:
            source_engine = list(view.keys())[0]
            view = {'schema': kwargs['viewschema'], 'query': view[source_engine]}

        tables = []
        for source, subtables in parser.source_tables.items():
            tables += subtables

        returns = []
        donereturns = {}
        for col in parser.return_columns:
            for source,qcols in parser.query_columns.items():
                for qcol in qcols:
                    if qcol['column'] == col and col not in donereturns:
                        returns.append(qcol)
                        donereturns[col] = True
                    elif qcol['alias'] == col:
                        returns.append(qcol)
                        donereturns[col] = True

        join = parser.join_path.get('MULTI_SOURCE', None)
        if join:
            for col in join:
                join[col] = [re.sub(".*\.", '', t) for t in join[col]]

        subquery = {
            'view': view,
            'return': returns,
            'tables': tables,
            'columns': parser.table_columns,
            'column_idx': {t: parser.table_column_idx[t] for t in tables},
            'conditions': parser.table_conditions.get(source,None),
            'join': join,
            'groupby': parser.groupby,
            'orderby': parser.orderby,
            'limit': parser.limit,
            }

        logger.debug("arbiter submit query to {}\n{}\n".format(source_engine, pprint.pformat(subquery)))
        results[source_engine] = self.arbiter(source_engine, subquery, **kwargs)

        return results

    def arbiter(self, source_engine, subquery, **kwargs):
        return self.engines[source_engine].query(subquery, **kwargs)

    def create(self, table):
        self.writeback.create(table)

    def write(self, table, df):
        self.writeback.write(table, df)

    def execute(self, sqlcmd):
        self.engines['postgres'].execute(sqlcmd)

