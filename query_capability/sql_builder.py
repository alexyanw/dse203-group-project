from datalog_parser import DatalogParser
__all__ = ['SQLBuilder']

class SQLBuilder:
    def __init__(self, q):
        self.parser = DatalogParser(q)

    def getReturnColumns(self):
        return_columns = []
        for col in self.parser.cols_to_return:
            table_name = self.parser.column_names[col]
            if table_name.find(";") != -1:
                table_name = table_name.split(";")[0]
            return_columns.append(table_name + "." + col)
        return ', '.join(return_columns)

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
        dbcmd = "SELECT " + self.getReturnColumns() + \
               " FROM " + ', '.join(self.parser.table_names)
        conditions = []
        join_cond = self.getJoinConditions()
        if join_cond: conditions.append(join_cond)
        conditions += self.parser.conds
        if conditions:
            dbcmd += " WHERE " + ' AND '.join(conditions)
        dbcmd += " LIMIT 10;"
        return dbcmd

    def getTableNames(self):
        return self.parser.table_names

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
