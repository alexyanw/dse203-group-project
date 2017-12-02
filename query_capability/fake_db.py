import pandas as pd

class FakeDB:
    ''' process dataframes in memory '''
    def __init__(self, data):
        self.mem_data = data
        self.source = 'view'

    def query(self, qstruct, **kwargs):
        filtered_results = {}
        if not qstruct['conditions']:
            filtered_results = self.mem_data
        else:
            for table in self.mem_data:
                filtered_results[table] = self.filter_by_condition(self.mem_data[table], qstruct['conditions'].get(table,None))

        df_result = self.resolve_join(filtered_results, qstruct)
        if qstruct['groupby']: 
            df_result = self.resolve_aggregation(df_result, qstruct['groupby'], qstruct['aggregation'])
        return df_result

    def filter_by_condition(self, df_in, conditions):
        if not conditions: return df_in
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

    def resolve_join(self, results, join_columns):
        if len(results) == 1:
            first = next(iter(results))
            return results[first]

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

