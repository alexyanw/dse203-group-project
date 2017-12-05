from hybrid_engine import HybridEngine


datalog1 = [
        {
        'result': 'Ans(nodeid)',
        'table': ['asterix.CategoryLevel(nodeid, level_1, level_2, level_3, level_4, level_5)'],
        'condition': ["level_1 = 'Education'"]
        }
    ]

datalog = [
        {
        'result': 'Ans(nodeid)',
        'table': ['asterix.CategoryFlat(nodeid, category)'],
        'condition': ["category = 'Education;Children & Teens'"],
        'limit': '10',
        }
    ]

#engine = HybridEngine()
engine = HybridEngine(
                postgres= {'server': 'localhost', 'port': 5432, 'database': 'SQLBook', 'user': 'postgres', 'password': 'pavan007'},
                asterix= {'server': 'localhost', 'port': 19002, 'dataverse': 'TinySocial'},
                solr= {'server': 'localhost', 'port': 8983, 'core': 'bookstore'})
#result = engine.queryDatalog(datalog, debug=True)
result = engine.queryDatalog(datalog)
print(result)
