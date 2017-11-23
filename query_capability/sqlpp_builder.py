import re
from category import Category

__all__ = ['SQLPPBuilder']

class SQLPPBuilder:
    def __init__(self, q, schema=None):
        self.datalog = q
        self.schema = schema

    def getColumnName(self, table, col_or_val, full=True):
        if not self.schema:
            return col_or_val
        if col_or_val not in self.datalog['column_idx'][table]:
            return col_or_val
        col_idx = self.datalog['column_idx'][table][col_or_val]
        if full:
            return table + '.' + self.schema[table].getColumn(table, col_idx)
        else:
            return self.schema[table].getColumn(table, col_idx)

    def getReturnColumns(self):
        return_columns = []
        for struct in self.datalog['return']:
            src_col = self.getColumnName(struct['table'], struct['column'], False)
            str_ret_col = struct['table'] + '.' + src_col 
            if struct['func']:
                str_ret_col = struct['func'] + '(' + str_ret_col + ')'
            if struct['alias']:
                str_ret_col += ' as ' + struct['alias'] 
            elif src_col != struct['column']:
                str_ret_col += ' as ' + struct['column'] 
            return_columns.append(str_ret_col)
        return ', '.join(return_columns)

    def getJoinConditions(self):
        if not self.datalog['join']: return ''
        join_columns = self.datalog['join']
        return_str = ""
        for col in join_columns:
            tables = join_columns[col]
            base_str = self.getColumnName(tables[0], col) + "="
            for table in tables[1:]:
                return_str += (base_str + self.getColumnName(table, col) + " AND ")
        return return_str[:-5]

    def resolveCondition(self, lop, relation, rop, special_handler):
        if lop in special_handler:
            return special_handler[lop](lop, relation, rop)
        else:
            return lop + ' ' + op + ' ' + rop

    def getArithmeticConditions(self, cond_handle=None):
        arith_conds = []
        if not self.datalog['conditions']: return arith_conds
        for table,conditions in self.datalog['conditions'].items():
            if not conditions: continue
            for cond in conditions:
                lop = self.getColumnName(table, cond[0])
                rop = self.getColumnName(table, cond[2])
                str_cond = self.resolveCondition(lop, cond[1], rop, cond_handle)
                arith_conds.append(str_cond)
        return arith_conds

    def getQueryCmd(self, cond_handle):
        dbcmd = "SELECT " + self.getReturnColumns() + \
               " FROM " + ', '.join(self.datalog['tables'])
        conditions = []
        join_cond = self.getJoinConditions()
        if join_cond: conditions.append(join_cond)
        conditions += self.getArithmeticConditions(cond_handle)
        if conditions:
            dbcmd += "\nWHERE " + ' AND '.join(conditions)
        if self.datalog['limit']:
            dbcmd += "\nLIMIT " + self.datalog['limit']
        return dbcmd
