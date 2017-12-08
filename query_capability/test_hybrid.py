from hybrid_engine import HybridEngine
from datalog_parser_frontend import DatalogParserFE
import logging

# multi source join
datalog = [
    {
    'result': 'Ans(pid, nunits, date, asin, nodeid)',
    'table': ['postgres.orders(oid, _, _, _, _, _, _, _, _, _, _)',
            'postgres.orderlines(olid, oid, pid, _, date, _, nunits, _)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.categoryflat(nodeid, category)',
           ],
    'condition': ['pid > 1000', "date > '2015-01-01'", "category = 'Education;Children & Teens'"],
    'limit': '10'
    }
]

# multi source aggregation
datalog5 = [
    {
    'result': 'Ans(lvl1, total_orders, total_value)',
    'table': ['postgres.orders(oid, _, _, _, _, _, _, _, _, _, _)',
            'postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.categorylevel(nodeid, lvl1, _, _, _, _)',
           ],
    'condition': ['pid < 10100', "date > '2015-01-01'"],
    'groupby': { 'key': 'lvl1', 'aggregation': ['count(oid, total_orders)', 'sum(price, total_value)']},
    'limit': '10'
    }
]

# union of single source
datalog4 = [
    {
    'result': 'Ans(pid, asin, total_value)',
    'table': ['postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
           ],
    'condition': ['pid < 10010'],
    'groupby': { 'key': 'pid', 'aggregation': ['sum(price, total_value)']},
    },
    {
    'result': 'Ans(pid, asin, total_value)',
    'table': ['postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
           ],
    'condition': ['pid > 10015', 'pid < 10020'],
    'groupby': { 'key': 'pid', 'aggregation': ['sum(price, total_value)']},
    },
]


# union of multi source & aggregation
datalog = [
    {
    'result': 'Ans(lvl1, total_orders, total_value)',
    'table': ['postgres.orders(oid, _, _, _, _, _, _, _, _, _, _)',
            'postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.categorylevel(nodeid, lvl1, _, _, _, _)',
           ],
    'condition': ['pid < 10100', "date > '2015-01-01'"],
    'groupby': { 'key': 'lvl1', 'aggregation': ['count(oid, total_orders)', 'sum(price, total_value)']},
    },
    {
    'result': 'Ans(lvl1, total_orders, total_value)',
    'table': ['postgres.orders(oid, _, _, _, _, _, _, _, _, _, _)',
            'postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.categorylevel(nodeid, lvl1, _, _, _, _)',
           ],
    'condition': ['pid > 10050', 'pid < 10200', "date > '2015-01-01'"],
    'groupby': { 'key': 'lvl1', 'aggregation': ['count(oid, total_orders)', 'sum(price, total_value)']},
    }
]

# view resolved in combiner
datalog4 = [
    {
    'result': 'temp(pid, cat, nunits, price)',
    'table': ['postgres.orderlines(olid, oid, pid, _, date, _, nunits, price)',
            'postgres.products(pid, _, _, _, _, _, asin, nodeid)',
            'asterix.categorylevel(nodeid, cat, _, _, _, _)',
           ],
    'condition': ['pid < 10100', "date > '2015-01-01'"],
    },
    {
    'result': 'Ans(cat, total_cat_value)',
    'table': ['view.temp(pid, cat, nunits, price)',
           ],
    'condition': ['nunits > 5'],
    'groupby': { 'key': 'cat', 'aggregation': ['sum(price, total_cat_value)']},
    }
]

datalog4 = '''
product_cat_view(pid, cat, nunits, price) <-
  postgres.orderlines(olid, oid, pid, _, date, _, nunits, price),
  postgres.products(pid, _, _, _, _, _, asin, nodeid),
  asterix.categorylevel(nodeid, cat, _, _, _, _),
  pid < 10100, date > '2015-01-01'
Ans(cat, total_cat_value) <-
  setof({pid}, {nunits}, {price}, view.product_cat_view(pid, cat, nunits, price), S), sum(price, total_cat_value)
'''
# using local server, everything default
#engine = HybridEngine()
engine = HybridEngine(
                postgres= {'server': 'localhost', 'port': 5432, 'database': 'SQLBook', 'user': 'postgres', 'password': 'pavan007'},
                asterix= {'server': 'localhost', 'port': 19002, 'dataverse': 'TinySocial'},
                solr= {'server': 'localhost', 'port': 8983, 'core': 'bookstore'})

result = engine.queryDatalog(datalog, loglevel=logging.DEBUG)
#result = engine.queryDatalogRaw(datalog, loglevel=logging.DEBUG)
print(result)
