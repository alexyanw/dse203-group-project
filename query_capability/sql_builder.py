__all__ = ['SQLBuilder']
import re

class SQLBuilder:
    def __init__(self, q, schema=None):
        self.datalog = q
        self.schema = schema

    def getColumnName(self, table, col_or_val, **kwargs):
        if kwargs.get('schema', None):
            return self.getViewColumn(table, kwargs['schema'], col_or_val, kwargs.get('full', True))
        if not self.schema:
            return col_or_val
        if col_or_val not in self.datalog['column_idx'][table]:
            return col_or_val
        col_idx = self.datalog['column_idx'][table][col_or_val]
        if kwargs.get('full', True):
            return table + '.' + self.schema[table].getColumn(table, col_idx)
        else:
            return self.schema[table].getColumn(table, col_idx)

    def getViewColumn(self, table, schema, col_or_val, full=True):
        if col_or_val not in schema:
            return col_or_val
        col_idx = self.datalog['column_idx'][table][col_or_val]
        if full:
            return table + '.' + schema[col_idx]
        else:
            return schema[col_idx]

    def getReturnColumns(self, datalogview=None):
        return_columns = []
        for struct in self.datalog['return']:
            table,column = struct['table'], struct['column']
            src_col = None
            schema = None
            if datalogview and table in datalogview['schema']:
                schema = datalogview['schema'][table]
            src_col = self.getColumnName(table, column, full=False, schema=schema)
            str_ret_col = table + '.' + src_col 
            if struct['func']:
                str_ret_col = struct['func'] + '(' + str_ret_col + ')'
            if struct['alias']:
                str_ret_col += ' as ' + struct['alias'] 
            elif src_col != column:
                str_ret_col += ' as ' + column 
            return_columns.append(str_ret_col)
        return ', '.join(return_columns)

    def getJoinConditions(self, viewschema=None):
        if not self.datalog['join']: return ''
        join_columns = self.datalog['join']
        return_str = ""
        for col in join_columns:
            tables = join_columns[col]
            base_str = self.getColumnName(tables[0], col, schema=viewschema.get(tables[0], None)) + "="
            for table in tables[1:]:
                return_str += (base_str + self.getColumnName(table, col, schema=viewschema.get(table, None)) + " AND ")
        return return_str[:-5]

    def getArithmeticConditions(self, datalogview=None):
        arith_conds = []
        if not self.datalog['conditions']: return arith_conds
        for table,conditions in self.datalog['conditions'].items():
            if not conditions: continue
            schema = None
            if datalogview and table in datalogview['schema']:
                schema = datalogview['schema'][table]
            for cond in conditions:
                lop = self.getColumnName(table, cond[0], schema=schema)
                rop = self.getColumnName(table, cond[2], schema=schema)
                if not re.search("'.*'", rop): rop = "'"+rop+"'"
                op = cond[1]
                arith_conds.append("{} {} {}".format(lop, op, rop))
        return arith_conds

    def getQueryCmd(self, datalogview=None):
        viewschema = datalogview.get('schema', None) if datalogview else {}
        dbcmd = "SELECT " + self.getReturnColumns(datalogview) + \
               "\nFROM " + ', '.join(self.datalog['tables'])
        conditions = []
        join_cond = self.getJoinConditions(viewschema)
        if join_cond: conditions.append(join_cond)
        conditions += self.getArithmeticConditions(datalogview)
        if conditions:
            dbcmd += "\nWHERE " + ' AND '.join(conditions)
        if self.datalog['groupby']:
            grouptable = self.datalog['groupby']['table']
            groupcolumn = self.datalog['groupby']['column']
            schema = None
            if datalogview and grouptable in datalogview['schema']:
                schema = datalogview['schema'][grouptable]
            dbcmd += "\nGROUP BY {} ".format(self.getColumnName(grouptable, groupcolumn, schema=schema))
        if self.datalog['orderby']:
            dbcmd += "\nORDER BY {} ".format(self.datalog['orderby']) 
        if self.datalog['limit']:
            dbcmd += "\nLIMIT " + self.datalog['limit']
        return dbcmd

