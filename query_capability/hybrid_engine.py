import re
import pandas as pd
from postgres_engine import PostgresEngine
from asterix_engine import AsterixEngine
from solr_engine import SolrEngine
from datalog_parser import DatalogParser

__all__ = ['HybridEngine']

class HybridEngine:
    def __init__(self, **kwargs):
        self.engines = {}
        self.engines['postgres'] = PostgresEngine(kwargs['postgres']) if 'postgres' in kwargs else PostgresEngine()
        self.engines['asterix'] = AsterixEngine(kwargs['asterix']) if 'asterix' in kwargs else AsterixEngine()
        self.engines['solr'] = SolrEngine(kwargs['solr']) if 'solr' in kwargs else SolrEngine()

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

    def resolveReturnColumns(self, parser):
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
                    if self.single_source:
                        table_columns.append({'table':table,'column':src_col,'func':func,'alias':col})
                    else:
                        table_columns.append({'table':table,'column':src_col,'func':None,'alias':None})
                        return_columns['MULTI_SOURCE'].append({'table':table,'column':src_col,'func':func,'alias':col})

            for col in self.join_path['MULTI_SOURCE']:
                table = parser.column_to_table[col].get(source, None)
                if table and col not in columns:
                    table_columns.append({'table':table,'column':col,'func':None,'alias':None})
            return_columns[source] = table_columns
        return return_columns

    def arbiter(self, parser, **kwargs):
        results = {}
        for source, tables in parser.source_tables.items():
            subquery = {
                #'return': [parser.column_to_table[c]+'.'+c for c in parser.return_columns if parser.column_to_table[c] in tables ],
                'return': self.return_columns[source],
                'tables': tables,
                'columns': parser.table_columns,
                'column_idx': {t: parser.table_column_idx[t] for t in tables},
                'conditions': parser.table_conditions.get(source,None),
                'join': self.join_path.get(source, None),
                'groupby': parser.groupby if self.single_source else None,
                'limit': parser.limit if self.single_source else None,
                }
            results[source] = self.engines[source].queryDatalog(subquery, **kwargs)
        return results 

    def combine(self, results, parser, **kwargs):
        if self.single_source:
            return list(results.values())[0]
        result = self.multiSourceJoin(results, self.join_path['MULTI_SOURCE'])
        if parser.groupby: result = self.multiSourceAggregate(result, parser.groupby, parser.aggregation)
        return result

    def queryDatalog(self, datalog, **kwargs):
        parser = DatalogParser(datalog)
        self.single_source = True if len(parser.source_tables) == 1 else False
        self.join_path = self.resolveJoinPath(parser.join_columns, parser.source_tables)
        self.return_columns = self.resolveReturnColumns(parser)
        if 'debug' in kwargs: 
            print("###### return columns #######")
            print(self.return_columns, "\n")

            print("###### join path #######")
            print(self.join_path, "\n")

        sub_results = self.arbiter(parser, **kwargs)
        if 'debug' in kwargs: 
            self.debugPrint(sub_results)
            return None
        return self.combine(sub_results, parser, **kwargs)

    def debugPrint(self, sub_results):
        for source in sub_results:
            print("###### query for", source, "#######")
            print(sub_results[source], "\n")

    def multiSourceJoin(self, results, join_columns):
        df_result = None
        for col,tables in join_columns.items():
            sources = set()
            for table in tables:
                source,table = table.split('.')
                sources.add(source)

            for source in sources:
                if df_result is None:
                    df_result = results[source]
                    df_result[col] = df_result[col].apply(str)
                else:
                    df_right = results[source]
                    df_right[col] = df_right[col].apply(str)
                    df_result = df_result.merge(df_right, on=col)
        return df_result

    def multiSourceAggregate(self, df_in, groupby, aggregation):
        ''' self.groupby = {'source': source, 'table':table, 'column': groupkey}
            self.aggregation[tgt_col] = (func, src_col) '''
        agg_param = {}
        rename_col = {}
        for tgt in aggregation:
            func, src_col = aggregation[tgt]
            agg_param[src_col] = [func]
            rename_col[src_col] = tgt
            if func == 'sum':
                df_in[src_col] = df_in[src_col].replace('[^\d]', '', regex=True).astype(float)

        df_group = df_in.groupby(groupby['column'])
        df_result = df_group.agg(agg_param).reset_index()
        df_result.columns = df_result.columns.droplevel(level=1)
        return df_result.rename(columns=rename_col)
        return df_result
