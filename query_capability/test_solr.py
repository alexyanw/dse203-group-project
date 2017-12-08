from hybrid_engine import HybridEngine
import logging

#ReviewText(reviewid, avg_word_length, number_word_capital, number_exlamation_question, avg_sentence_length, tfidf_100, reviewText)
# get just one feature
datalog = [
        {
        'result': 'Ans(asin, tfidf)',
        'table': ['solr.review_text(reviewid, asin, length, avg_word_length, number_word_capital, number_exlamation_question, avg_sentence_length, tfidf, reviewText)'],
        'condition': ["length > 300"],
        'limit': '2',
        }
    ]

# get multiple features
datalog = [
        {
        'result': 'Ans(reviewid, asin, length, avg_word_length, number_word_capital, number_exlamation_question, avg_sentence_length, tfidf)',
        'table': ['solr.review_text(reviewid, asin, length, avg_word_length, number_word_capital, number_exlamation_question, avg_sentence_length, tfidf, reviewText)'],
        'condition': ["length > 300"],
        'limit': '10000',
        }
    ]

engine = HybridEngine(
                postgres= {'server': 'localhost', 'port': 5432, 'database': 'SQLBook', 'user': 'postgres', 'password': ''},
                asterix= {'server': 'localhost', 'port': 19002, 'dataverse': 'TinySocial'},
                solr= {'server': 'localhost', 'port': 8983, 'core': 'bookstore'})

result = engine.queryDatalog(datalog, loglevel=logging.DEBUG)
print(result)
