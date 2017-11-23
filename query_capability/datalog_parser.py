import sys
from sqlalchemy import create_engine
from functools import reduce
import re

__all__ = ['DatalogParser']

class DatalogParser:
    def __init__(self, q):
        q0 = q[0]
        self.result = q0['result']          # Ans(numunits, firstname, billdate)
        self.conditions = q0['condition']    # ['orders.orderid > 1000', 'orders.numunits > 1']

        tables = q0['table'] # ['orders(numunits, customerid, orderid)', 'customers(firstname, customerid)', 'orderlines(billdate, orderid)']

        self.source_tables = {}
        self.table_columns = {}
        self.table_column_idx = {}
        self.column_to_table = {}
        self.resolveSourceTables(tables)
        self.return_columns = self.getReturnColumns(self.result)         #numunits, firstname, billdate

        self.join_columns = self.getJoinColumns(self.table_columns)
        self.table_conditions = self.getTableConditions(self.conditions)
        #self.resolveNegation()

        self.groupby = None
        self.aggregation = None
        self.resolveAggregation(q0['groupby'])
        self.limit = q0.get('limit', None)

        self.validate()

    def resolveAggregation(self, groupby):
        ''' 'groupby': { 'key': 'pid', 'aggregation': ['count(oid, total_orders)', 'sum(price, total_value)']},'''
        if not groupby: return
        if 'key' not in groupby or 'aggregation' not in groupby:
            print("groupby must be dict of {key: ..., aggregation: ...}\n")
            exit(1)
        groupkey,aggs = groupby['key'], groupby['aggregation']
        source,table = list(self.column_to_table[groupkey].items())[0]
        self.groupby = {'source': source, 'table':table, 'column': groupkey}
        
        self.aggregation = {}
        for agg in aggs:
            match = re.search("(\S+)\((\S+),(\S+)\)", re.sub("\s", '', agg))
            if match:
                func,src_col,tgt_col = match.group(1), match.group(2), match.group(3)
                self.aggregation[tgt_col] = (func, src_col)

    def validate(self):
        return True
       #for col in self.return_columns:
       #    if col not in self.column_to_table:
       #        raise Exception("return column '{}' in header doesn't exist in body!".format(col))

    def resolveSourceTables(self, tables):
        self.source_tables = {}
        self.column_to_table = {}
        for table in tables:
            match = re.search("(\w+)\.(\w+)\((.*)\)", table)
            if not match:
                print("datalog table '{}' must follow pattern <source>.<table>(col1, col2, ...)\n".format(table))
                exit(1)
            source, tablename, str_columns = match.group(1), match.group(2), match.group(3)
            self.table_column_idx[tablename] = {}
            if source not in self.source_tables: self.source_tables[source] = []
            self.source_tables[source].append(tablename)
            self.table_columns[tablename] = re.sub('\s','', str_columns).split(',')
            for idx,col in enumerate(self.table_columns[tablename]):
                self.table_column_idx[tablename][col] = idx
                if col not in self.column_to_table: self.column_to_table[col] = {}
                self.column_to_table[col][source] = tablename

    # Given a datalog query of form - "Ans(numunits, firstname, billdate), orders.orderid > 1000, orders.numunits > 1"
    # this method extracts the column names from it
    def getReturnColumns(self, datalog):
        return [col.strip() for col in datalog[(datalog.index("(")+1): datalog.index(")")].split(",")]

    # Dictionary of {numunits:orders, customerid:orders;customers} etc.
    def getJoinColumns(self, table_columns):
        col_map = {}
        for table,columns in table_columns.items():
            for col in columns:
                if col == '_': continue
                if col in col_map:
                    col_map[col].append(table)
                else:
                    col_map[col] = [table]

        return {col: col_map[col] for col in col_map if len(col_map[col]) > 1}

    def getTableConditions(self, conditions):
        cond_map = {}
        for cond in conditions:
            match = re.search("(\S+)\s*([>=<]+)\s*(.+)", cond)
            if not match:
                print("bad condition pattern: {}".format(cond))
                exit(1)
            lop, op, rop = match.group(1), match.group(2), match.group(3)
            # HACK: suppose only 1 operand is column, and pure column in condition 
            tables = []
           #match = re.search("(\w+)\.(\w+)", lop)
           #if match: lop = match.group(2)
           #match = re.search("(\w+)\.(\w+)", rop)
           #if match: rop = match.group(2)

            if lop in self.column_to_table:
                source_tables = self.column_to_table[lop]
            else:
                source_tables = self.column_to_table[rop]

            for source,table in source_tables.items():
                if source not in cond_map: cond_map[source] = {}
                if table not in cond_map: cond_map[source][table] = []
                cond_map[source][table].append([lop, op, rop])
        return cond_map

    def resolveNegation(self):
        negation_map = {}
        negation_values = {}
        for val in self.conditions:
            match = re.search('not (\S+)\((.*)\)', val)
            if not match: continue
            table, columns = match.group(1), match.group(2)
            for idx, col in enumerate(columns.split(',')):
                match = re.search('"(.*)"', col)
                if match:
                    negation_map[table+':'+str(idx)] = match.group(1)
                    continue
                match = re.search('(\d+)', col)
                if match:
                    negation_map[table+':'+str(idx)] = match.group(1)

        for key,neg_value in negation_map.items():
            table, idx = key.split(':')
            negation_values[table] = { self._table_columns[table][int(idx)]: neg_value}
        
       #for table,negs in self.negation_values.items():
       #    for n,v in negs.items():
       #        self.conds.append("{}.{} != '{}'".format(table, n, v))

