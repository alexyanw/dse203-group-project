from hybrid_engine import HybridEngine
import pandas as pd
#RecommendationContent(productid, category, rank) <= [
#        {'producta', 'cat1', 1},
#        {'productb', 'cat1', 1},
#        {'productc', 'cat1', 1},
#        {'productd', 'cat1', 1},
#]
df = pd.DataFrame({
	'productid': [10001, 10002, 10003],
        'category': ['cat1', 'cat1', 'cat2'],
        'rank': [1, 2, 1],
        })
engine = HybridEngine()
#engine.writeback('RecommendationContent', df)
engine.create('RecommendationColaborative')
