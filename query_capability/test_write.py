from hybrid_engine import HybridEngine
import pandas as pd

df = pd.DataFrame({
	'productid': [10001, 10002, 10003],
        'category': ['cat1', 'cat1', 'cat2'],
        'rank': [1, 2, 1],
        })
#engine = HybridEngine()
engine = HybridEngine(
                postgres= {'server': 'localhost', 'port': 5432, 'database': 'SQLBook', 'user': 'postgres', 'password': 'pavan007'},
                asterix= {'server': 'localhost', 'port': 19002, 'dataverse': 'TinySocial'},
                solr= {'server': 'localhost', 'port': 8983, 'core': 'bookstore'})

#engine.execute('drop table RecommendationColaborative')
#engine.create('RecommendationColaborative')
#engine.write('RecommendationColaborative', df)

datalog = [{
        'result': 'Ans(productid, category, rank)',
        'table': ['postgres.recommendationcolaborative(productid, category, rank)'],
    }]
# using local server, everything default
#engine = HybridEngine()
engine = HybridEngine(
                postgres= {'server': 'localhost', 'port': 5432, 'database': 'SQLBook', 'user': 'postgres', 'password': 'pavan007'},
                asterix= {'server': 'localhost', 'port': 19002, 'dataverse': 'TinySocial'},
                solr= {'server': 'localhost', 'port': 8983, 'core': 'bookstore'})
#result = engine.queryDatalog(datalog, debug=True)
result = engine.queryDatalog(datalog)
print(result)
