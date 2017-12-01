class Combiner:
    def __init__(self, mode, prev_results, parsers):
        self.mode = mode
        self.source_results = prev_results
        self.datalog_parsers = parsers

    def process(self):
        if self.mode == 'single':
            return self.resolve_single_query()

    def resolve_single_query(self, **kwargs):
        sub_results = self.source_results[0]
        parser = self.datalog_parsers[0]
        return self.combine(sub_results, parser, **kwargs)

    def combine(self, results, parser, **kwargs):
        if parser.single_source():
            return list(results.values())[0]
        result = self.multiSourceJoin(results, parser.join_path['MULTI_SOURCE'])
        if parser.groupby: result = self.multiSourceAggregate(result, parser.groupby, parser.aggregation)
        return result

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


    def union(self):
        None
