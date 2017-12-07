import pandas as pd
import logging, pprint
from sql_builder import SQLBuilder
from source_schema import SourceTable
from writeback import Writeback
from product_view import ProductView
from customer_view import CustomerView
from cooccurrence_matrix import CoOccurrenceMatrix
from sqlalchemy import create_engine,Table,MetaData
from sqlalchemy.sql import text
from utils import *

__all__ = ['PostgresEngine']

logger = logging.getLogger('qe.PostgresEngine')

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
        for table in SourceTable.schema:
            self.schema_wrapper[table] = SourceTable
        for table in Writeback.schema:
            self.schema_wrapper[table] = Writeback

    def executeQuery(self, cmd, **kwargs):
        if 'debug' in kwargs:
            return cmd
        self.pg_conn = create_engine(self.dburl)
        df = pd.read_sql_query(cmd, self.pg_conn)
        logger.debug("query sample result:\n{}\n".format(pprint.pformat(df[:5])))
        return df

    def query(self, datalog, **kwargs):
        views = []
        view_queries = []
        if 'view' in datalog and datalog['view']:
            view_queries.append(datalog['view']['query'])
            views += list(datalog['view']['schema'].keys())

        builder = SQLBuilder(datalog, self.schema_wrapper)
        for table in datalog['tables']:
            if table in views: continue
            if self.schema_wrapper[table] in [SourceTable, Writeback]: continue
            wrapper_class = self.schema_wrapper[table]
            #features = [col['column'] for col in datalog['return']]
            view_queries += wrapper_class().get_views(table=table, view=True, **kwargs)
        sqlcmd = builder.getQueryCmd(datalog['view'])

        if 'returnview' in kwargs:
            sqlcmd = "{} as ({})".format(kwargs['returnview'], sqlcmd)
            if view_queries:
                sqlcmd = "{},\n{}".format(",\n".join(view_queries), sqlcmd)
            return sqlcmd

        if view_queries:
            sqlcmd = "WITH {}\n{}".format(",\n".join(view_queries), sqlcmd)

        logger.info("query sql cmd:\n{}\n".format(sqlcmd))
        return self.executeQuery(sqlcmd, **kwargs)

    def get_schema(self, table):
        sqlcmd = "select column_name, data_type from information_schema.columns where table_name = '{}'".format(table.lower())
        result = self.execute(sqlcmd)
        schema = {}
        for row in result:
            schema[row[0]] = row[1]

        return schema

   #def get_schema(self, table):
   #    sqlcmd = "select 1 from INFORMATION_SCHEMA.views WHERE table_name = '{}'".format(table.lower())
   #    exists = self.execute(sqlcmd)
   #    print("view exists")
   #    if not exists:
   #        sqlcmd = "select 1 from INFORMATION_SCHEMA.tables WHERE table_name = '{}'".format(table.lower())
   #        exists = self.execute(sqlcmd)
   #    if not exists:
   #        fatal("table '{}' not exists in postgres".format(table))

   #    sqlcmd = "select column_name, data_type from information_schema.columns where table_name = '{}'".format(table.lower())
   #    result = self.execute(sqlcmd)
   #    return result

    def create_table(self, table, schema):
        sqlcmd = "CREATE TABLE {}(\n".format(table)
        cols = []
        for item in schema:
            col,type = item
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
        self.pg_conn.execute(sqltb.insert(), data)
