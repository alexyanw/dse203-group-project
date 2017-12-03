import pandas as pd
from sql_builder import SQLBuilder
from source_schema import SourceTable
from product_view import ProductView
from customer_view import CustomerView
from cooccurrence_matrix import CoOccurrenceMatrix
from sqlalchemy import create_engine,Table,MetaData
from sqlalchemy.sql import text


__all__ = ['PostgresEngine']

class PostgresEngine:
    def __init__(self, cfg={}):
        server = cfg.get('server', '132.249.238.27')
        port = cfg.get('port', 5432)
        database = cfg.get('database', 'bookstore_pr')
        user = cfg.get('user', 'student')
        passwd = cfg.get('password', '123456')
        self.dburl = 'postgresql://' + user + ':' + passwd + '@' + server + ':' + str(port) + '/' + database
        self.schema_wrapper = {
            'product_view': ProductView,
            'product_orders': ProductView,
            'customer_product': CoOccurrenceMatrix,
            'cooccurrence_matrix': CoOccurrenceMatrix,
            'products': SourceTable,
            'customers': SourceTable,
            'orders': SourceTable,
            'orderlines': SourceTable,
            'campaigns': SourceTable,
            'reviews': SourceTable,
            'zipcensus': SourceTable,
            'zipcounty': SourceTable,
        }

    def executeQuery(self, cmd, **kwargs):
        if 'debug' in kwargs:
            return cmd
        self.pg_conn = create_engine(self.dburl)
        df = pd.read_sql_query(cmd, self.pg_conn)
        return df

    def queryView(self, view, features, **kwargs):
        if view not in self.schema_wrapper: 
            print("Error: view '{}' not defined".format(view))
            exit(1)

        return self.schema_wrapper[table]().get_views(features, **kwargs)

    def query(self, datalog, **kwargs):
        builder = SQLBuilder(datalog, self.schema_wrapper)
        views = []
        for table in datalog['tables']:
            if self.schema_wrapper[table] == SourceTable: continue
            wrapper_class = self.schema_wrapper[table]
            views += wrapper_class().get_views(table=table, view=True,**kwargs)
        sqlcmd = builder.getQueryCmd()

        if views:
            sqlcmd = "WITH {}\n{}".format(",\n".join(set(views)), sqlcmd)

        return self.executeQuery(sqlcmd, **kwargs)

    def get_schema(self, table):
        sqlcmd = "select column_name, data_type from information_schema.columns where table_name = '{}'".format(table.lower())
        result = self.execute(sqlcmd)
        schema = {}
        for row in result:
            print(row, type(row))
            schema[row[0]] = row[1]

        return schema

    def create_table(self, table, schema):
        sqlcmd = "CREATE TABLE {}(\n".format(table)
        cols = []
        for col,type in schema.items():
            cols.append("{} {}".format(col, type))
        sqlcmd += ",\n".join(cols) + "\n)"
        print(sqlcmd)

        self.execute(sqlcmd)

    def execute(self, sqlcmd):
        statement = text(sqlcmd)
        self.pg_conn = create_engine(self.dburl)
        return self.pg_conn.execute(statement)

    def insert(self, table, df):
        self.pg_conn = create_engine(self.dburl)
        meta = MetaData()
        meta.reflect(bind=self.pg_conn)
        sqltb = meta.tables[table.lower()]
        data = list(df.T.to_dict().values())
        print(type(data[0]), data)
        self.pg_conn.execute(sqltb.insert(), data)
