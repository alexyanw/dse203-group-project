import pandas as pd
from sql_builder import SQLBuilder
from product_view import ProductView
from customer_view import CustomerView
from cooccurrence_matrix import CoOccurrenceMatrix

__all__ = ['PostgresEngine']

class PostgresEngine:
    def __init__(self, server = 'localhost', port = 5432, database = 'SQLBook'):
        self.dburl = 'postgresql://postgres:@' + server + ':' + str(port) + '/' + database
        self.schema_wrapper = {
            'product_view': ProductView,
            'product_orders': ProductView,
            'customer_product': CoOccurrenceMatrix,
            'cooccurrence_matrix': CoOccurrenceMatrix,
            'products': 'source',
            'customers': 'source',
            'orders': 'source',
            'orderlines': 'source',
            'campaigns': 'source',
            'reviews': 'source',
            'zipcensus': 'source',
            'zipcounty': 'source',
        }

    def execute(self, cmd, **kwargs):
        if 'debug' in kwargs:
            return cmd
        self.pg_conn = create_engine(dburl)
        df = pd.read_sql_query(dbcmd, self.pg_conn)
        return df

    def queryView(self, view, features, **kwargs):
        if view not in self.schema_wrapper: 
            print("Error: view '{}' not defined".format(view))
            exit(1)

        return self.schema_wrapper[table]().get_views(features, **kwargs)

    def queryDatalog(self, datalog, **kwargs):
        builder = SQLBuilder(datalog)
        views = []
        for table in builder.getTableNames():
            if self.schema_wrapper[table]  == 'source': continue
            wrapper_class = self.schema_wrapper[table]
            views += wrapper_class().get_views(table=table, view=True,**kwargs)
        sqlcmd = builder.getQueryCmd()

        if views:
            sqlcmd = "WITH {}\n{}".format(",\n".join(set(views)), sqlcmd)

        return self.execute(sqlcmd, **kwargs)


