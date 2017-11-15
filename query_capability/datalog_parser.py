import sys
from sqlalchemy import create_engine
from functools import reduce

__all__ = ['DatalogParser']

class DatalogParser:
    def __init__(self, q):
        self.datalog = q[0]                               #"Ans(numunits, firstname, billdate), orderid > x, numunits > 1"
        self.cols_to_return = self.getColumns(self.datalog)         #numunits, firstname, billdate
        q = q[1:]                                    #everything except datalog
        self.conds   = reduce(lambda x,y: x+y, [i for i in q if isinstance(i, list)], [])
        # conds includes the following:
        # ['orders.orderid > 1000', 'orders.numunits > 1']

        self.selects = [i for i in q if not isinstance(i, list)]
        # selects includes the following:
        # 'orders(numunits, customerid, orderid)',
        #'customers(firstname, customerid)',
        # 'orderlines(billdate, orderid)'

        self.table_names = self.getTableNames() 
        self.column_names = self.getJoinColumns() 

        self.validate()

    def validate(self):
        for col in self.cols_to_return:
            if col not in self.column_names:
                raise Exception("return column '{}' in header doesn't exist in body!".format(col))

    # Given a datalog query of form - "Ans(numunits, firstname, billdate), orders.orderid > 1000, orders.numunits > 1"
    # this method extracts the column names from it
    def getColumns(self, datalog):
        return [col.strip() for col in datalog[(datalog.index("(")+1): datalog.index(")")].split(",")]

    # Given a datalog query of form - "Ans(numunits, firstname, billdate), orders.orderid > 1000, orders.numunits > 1"
    # this method extracts the where conditions from it
    def getWhereConditions(self):
        rawString = self.datalog.split("),")[1]
        conds = [cond.strip() for cond in rawString.split(",")]
        return conds

    # Dictionary of {numunits:orders, customerid:orders;customers} etc.
    def getJoinColumns(self):
        col_map = {}
        for val in self.selects:
            table_name = val[:val.index("(")]
            columns = self.getColumns(val)
            for col in columns:
                if col in col_map:
                    col_map[col] = col_map.get(col) + ";" +table_name
                else:
                    col_map[col] = table_name
        return col_map

    # [orders, customers, orderlines]
    def getTableNames(self):
        return [i[:i.index("(")].strip() for i in self.selects]


    @staticmethod
    def prettyPrintResult(res):
        result_list = []
        for r in res:
            current = "Ans("
            for ele in r:
                current += str(ele) + ","
            current = current[:-1] 
            current += ")"
            result_list.append(current)
        return result_list


