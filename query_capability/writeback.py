import pandas as pd
import math
from utils import *

class Writeback:
    schema = {
           #'cooccurrencecache': [
           #    ('asin_purchased', 'varchar(128)'),
           #    ('asin_together', 'varchar(128)'),
           #    ('metric', 'varchar(128)'),
           #    ('catlvl1', 'varchar(128)'),
           #    ('catlvl2', 'varchar(128)'),
           #    ('catlvl3', 'varchar(128)'),
           #    ('catlvl4', 'varchar(128)'),
           #    ('catlvl5', 'varchar(128)'),
           #    ('season1', 'varchar(128)'),
           #    ('season2', 'varchar(128)'),
           #    ('season3', 'varchar(128)'),
           #    ('season4', 'varchar(128)'),
           #    ('price_asin_together', 'int'),
           #    ('demographic_region', 'int'),
           #    ('demographic_gender', 'varchar(64)'),
           #    ],
            'recommendationcolaborative': [
                ('productid', 'integer'),
                ('category', 'varchar(128)'),
                ('rank', 'integer')
                ],
            'content_based': [
                ('asin', 'varchar(16)'),
                ('rating', 'numeric'),
                ('spring', 'numeric'),
                ('summer', 'numeric'),
                ('winter', 'numeric'),
                ('fall', 'numeric'),
                ('fullprice', 'money'),
                ('isinstock', 'int'),
                ('lvl1', 'int'),
                ('lvl2', 'int'),
                ('lvl3', 'int'),
                ('lvl4', 'int'),
                ('lvl5', 'int')
		],
            'cooccurrence': [
		('asin', 'varchar(16)'),
		('asin_other', 'varchar(15)'),
		('metric', 'numeric'),
		('spring', 'numeric'),
		('summer', 'numeric'),
		('winter', 'numeric'),
		('fall', 'numeric'),
		('fullprice', 'money'),
		('isinstock', 'int'),
		('lvl1', 'int'),
		('lvl2', 'int'),
		('lvl3', 'int'),
		('lvl4', 'int'),
		('lvl5', 'int'),
		('demo_region', 'int'),
		('demo_gender', 'int'),
		]
            }

    def __init__(self, engine):
        self.engine = engine
        self.batch_size = 5000

    @classmethod
    def getColumn(cls, table, idx):
        if table not in cls.schema:
            fatal("writeback table '{}' doesn't exist\n".format(table))
        return cls.schema[table][idx][0]

    def create(self, table):
        if table not in self.schema:
            print("Error: can't create table not defined in writeback schema")
            exit(1)
        self.engine.create_table(table, self.schema[table])

    def write(self, table, df):
        schema = self.engine.get_schema(table)
        if not self.validate_schema(schema, df): exit(1)

        size = df.shape[1]
        for i in range(math.ceil(size/self.batch_size)):
            start = i * self.batch_size
            end = start + self.batch_size
            self.engine.insert(table, df[start:end])

    def validate_schema(self, schema, df):
        # check columns
        in_cols = set(df.columns)
        db_cols = set(schema.keys())
        if not in_cols.issubset(db_cols):
            miss = in_cols - db_cols
            print("Error: columns of input dataframe doesn't exist:", miss)
            return False

        # check types
        #dict(df.dtypes)
        return True

