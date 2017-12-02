import pandas as pd
from sql_builder import SQLBuilder
from source_schema import SourceTable
from product_view import ProductView
from customer_view import CustomerView
from cooccurrence_matrix import CoOccurrenceMatrix
from sqlalchemy import create_engine

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

    def execute(self, cmd, **kwargs):
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

        return self.execute(sqlcmd, **kwargs)


