from hybrid_engine import HybridEngine

#ReviewText(reviewid, avg_word_length, number_word_capital, ratio_exlamation_question, avg_sentence_length, tfidf_100, reviewText)
# get just one feature
datalog = [
        {
        'result': 'Ans(asin, reviewText)',
        'table': ['solr.review_text(reviewid, asin, length, avg_word_length, number_word_capital, ratio_exlamation_question, avg_sentence_length, tfidf_100, reviewText)'],
        'condition': ["length > 300"],
        'limit': '2',
        }
    ]

# get just multiple features
datalog1 = [
        {
        'result': 'Ans(asin, length, avg_word_length, tfidf_100)',
        'table': ['solr.review_text(reviewid, asin, length, avg_word_length, number_word_capital, ratio_exlamation_question, avg_sentence_length, tfidf_100, reviewText)'],
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
