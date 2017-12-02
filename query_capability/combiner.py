import pandas as pd
from fake_db import FakeDB

class Combiner:
    def __init__(self, mode, prev_results, parsers):
        self.mode = mode
        self.source_results = prev_results
        self.datalog_parsers = parsers

    def process(self, **kwargs):
        if self.mode == 'single':
            return self.resolve_single_query(**kwargs)
        if self.mode == 'union':
            return self.union(**kwargs)
        if self.mode == 'view':
            return self.resolve_view(**kwargs)

    def resolve_single_query(self, **kwargs):
        sub_results = self.source_results[0]
        parser = self.datalog_parsers[0]
        return self.combine(sub_results, parser, **kwargs)

    def combine(self, results, parser, **kwargs):
        if parser.single_source():
            return list(results.values())[0]
        result = self.resolve_join(results, parser.join_path['MULTI_SOURCE'])
        if parser.groupby: result = self.resolve_aggregation(result, parser.groupby, parser.aggregation)

        # filter result by return columns
        return result[parser.return_columns]

    def resolve_join(self, results, join_columns):
        ''' multi source join '''
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

    def resolve_aggregation(self, df_in, groupby, aggregation):
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

    def union(self, **kwargs):
        dfs = []
        for i in range(len(self.source_results)):
            source_result = self.source_results[i]
            parser = self.datalog_parsers[i]
            result = self.combine(source_result, parser, **kwargs)
            if 'debug' in kwargs: print(result)
            dfs.append(result)
        df_result = pd.concat(dfs)
        return df_result.drop_duplicates()

    def resolve_view(self, **kwargs):
        sub_results = self.source_results[0]
        parser = self.datalog_parsers[0]
        view_data = self.combine(sub_results, parser, **kwargs)

        parser = self.datalog_parsers[1]
        source = 'view'
        tables = parser.source_tables[source]
        query_struct = {
            'return': parser.query_columns[source],
            'columns': parser.table_columns,
            'column_idx': {t: parser.table_column_idx[t] for t in tables},
            'conditions': parser.table_conditions.get(source,None),
            'join': parser.join_path.get(source, None),
            'groupby': parser.groupby,
            'aggregation': parser.aggregation,
            'limit': parser.limit
            }

        db = FakeDB({parser.return_table: view_data})
        return db.query(query_struct, **kwargs)
