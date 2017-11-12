import sys
from sqlalchemy import create_engine
from functools import reduce


class DatalogParser:
    def __init__(self, q):
        self.datalog = q[0]                               #"Ans(numunits, firstname, billdate), orderid > x, numunits > 1"
        self.cols_to_return = self.getColumns(self.datalog)         #numunits, firstname, billdate
        q = q[1:]                                    #everything except datalog
        self.conds   = reduce(lambda x,y: x+y, [i for i in q if isinstance(i, list)])
        # conds includes the following:
        # ['orders.orderid > 1000', 'orders.numunits > 1']

        self.selects = [i for i in q if not isinstance(i, list)]
        # selects includes the following:
        # 'orders(numunits, customerid, orderid)',
        #'customers(firstname, customerid)',
        # 'orderlines(billdate, orderid)'

        self.table_names = self.getTableNames() 
        self.column_names = self.getJoinColumns() 


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
        col_names = {}
        for val in self.selects:
            table_name = val[:val.index("(")]
            columns = self.getColumns(val)
            for col in columns:
                if col in col_names:
                    table = col_names.get(col)
                    table += ";"+table_name
                    col_names[col] = table
                else:
                    col_names[col] = table_name
        return col_names

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


class SQLBuilder:
    def __init__(self, q):
        self.parser = DatalogParser(q)

    def getQueryColsFromFQCols(self):
        column_names = self.parser.column_names
        return_str = ""
        for col in self.parser.cols_to_return:
            if col in column_names:
                table_name = column_names[col]
                if table_name.find(";") == -1:
                    return_str += table_name + "." + col + ", "
                else:
                    table1 = table_name.split(";")[0]
                    return_str += table1 + "." + col + ", "
            else:
                raise Exception('Oh. Something bad happened!')
        return return_str[:-2]

    def getJoinConditions(self):
        column_names = self.parser.column_names
        return_str = ""
        for col in column_names:
            if column_names[col].find(";") != -1:
                tables = column_names[col].split(";")
                base_str = tables[0].strip() + "." + col + "="
                for table in tables[1:]:
                    return_str += (base_str + table.strip() + "." + col + " AND ")
        return return_str[:-5]

    def getQueryCmd(self):
        return "SELECT " + self.getQueryColsFromFQCols() + \
               " FROM " + ', '.join(self.parser.table_names) + \
               " WHERE " + self.getJoinConditions() + \
               " AND " + ' AND '.join(self.parser.conds) + " LIMIT 10;"

if __name__ == '__main__':
    example_datalog = [
        'Ans(numunits, firstname, billdate, orderid, customerid)',
        'orders(orderid, customerid, campaignId, orderDate, city, state, zipCode, paymentType, totalPrice, numOrderLines, numUnits)',
        'customers(customerid, householdId, gender, firstname)',
        'orderlines(orderLineId, orderid, productId, shipDate, billdate, unitPrice, numunits, totalPrice)',
        ['orders.orderid > 1000', 'orders.numunits > 1']
    ]
    
    builder = SQLBuilder(example_datalog)
    sqlcmd = builder.getQueryCmd()
    print(sqlcmd)
