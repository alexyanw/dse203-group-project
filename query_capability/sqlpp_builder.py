import re
from datalog_parser import DatalogParser
from category import Category

__all__ = ['SQLPPBuilder']

class SQLPPBuilder:
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

    def resolveCondition(self, cond, special_handler):
        match = re.search("(\S+)\s*([<>=!]+)\s*(\S+)", cond)
        if not match:
            print("Error: condition '{}' can't be parsed".format(cond))
            exit(1)
        col, relation, value = match.group(1), match.group(2), match.group(3)
        if col in special_handler:
            return special_handler[col](col, relation, value)
        else:
            return cond

    def getQueryCmd(self, special_handler):
        dbcmd = "SELECT " + self.getReturnColumns() + \
               " FROM " + ', '.join(self.parser.table_names)
        conditions = []
        join_cond = self.getJoinConditions()
        if join_cond: conditions.append(join_cond)
        for cond in self.parser.conds:
            conditions.append(self.resolveCondition(cond, special_handler))
        if conditions:
            dbcmd += " WHERE " + ' AND '.join(conditions)
        dbcmd += " LIMIT 10;"
        return dbcmd

    def getTableNames(self):
        return self.parser.table_names

