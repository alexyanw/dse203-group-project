from hybrid_engine import HybridEngine

#ReviewText(reviewid, avg_word_length, number_word_capital, ratio_exlamation_question, avg_sentence_length, ari, tfidf_100)
datalog = [
        {
        'result': 'Ans(asin, review)',
        'table': ['solr.review_text(reviewid, asin, length, avg_word_length, number_word_capital, ratio_exlamation_question, avg_sentence_length, ari, tfidf_100)'],
        'condition': ["length > 300"],
        'limit': '2',
        }
    ]

engine = HybridEngine(
                postgres= {'server': 'localhost', 'port': 5432, 'database': 'SQLBook', 'user': 'postgres', 'password': ''},
                asterix= {'server': 'localhost', 'port': 19002, 'dataverse': 'TinySocial'},
                solr= {'server': 'localhost', 'port': 8983, 'core': 'bookstore'})
result = engine.queryDatalog(datalog)
print(result)
