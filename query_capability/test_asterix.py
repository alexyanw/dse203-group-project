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

engine = HybridEngine()
query = engine.queryDatalog(datalog)
print(query['asterix'])
