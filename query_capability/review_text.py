import re
import pandas as pd
__all__ = ['ReviewText']

def list2dict(data):
    # convert : [u'tf', 1, u'df', 2, u'tf-idf', 0.5]
    # to a dict
    stop = len(data)
    keys = [data[i] for i in range(stop) if i%2==0]
    values = [data[i] for i in range(stop) if i%2==1]
    return dict(zip(keys,values))

class ReviewText:
    schema = {
        'review_text': ['reviewerID', 'asin', 'review_length', 'avg_word_length', 'number_word_capital', 'ratio_exlamation_question', 'avg_sentence_length', 'tfidf_100', 'reviewText']
    }
    @classmethod
    def getColumn(cls, table, idx):
        if table not in cls.schema:
            print("Required table '{}' doesn't exist\n".format(table))
            exit(1)
        return cls.schema[table][idx]

    def __init__(self):
        self.schema_map = {
            'review_text' : {
                'reviewerID': self.get_raw,
                'asin': self.get_raw,
                'review_length': self.get_review_length,
                'avg_word_length': self.get_avg_word_length,
                'number_word_capital': self.get_null,
                'ratio_exlamation_question': self.get_null,
                'avg_sentence_length': self.get_null,
                #'ari': self.get_null,
                'tfidf_100': self.get_tfidf,
                'reviewText': self.get_raw,
                }
        }
        self.term_vectors = None

    def get_null(self, solr_results):
        return None

    def get_raw(self, solr_results):
        result = pd.DataFrame(list(solr_results))
        return result

    def get_tfidf(self, solr_results):
        tvc = self.extract_term_vectors(solr_results.raw_response.get('termVectors', None), 'reviewText')

    def get_review_length(self, solr_results):
        result = pd.DataFrame(list(solr_results))
        result['review_length'] = result['reviewText'].apply(len)
        return result

    def get_punctuation_ratio(self, solr_results):
        tvc = self.extract_term_vectors(solr_results.raw_response.get('termVectors', None), 'reviewText')
        result = []
        for id,tv in tvc.items():
            word_count = sum([tv[w]['tf'] for w in tv])
            total_length = sum([tv[w]['tf']*len(w) for w in tv])
            result.append({'id': id, 'avg_word_length': total_length/word_count})
        return pd.DataFrame(result)

    def get_avg_word_length(self, solr_results):
        tvc = self.extract_term_vectors(solr_results.raw_response.get('termVectors', None), 'reviewText')
        result = []
        for id,tv in tvc.items():
            word_count = sum([tv[w]['tf'] for w in tv])
            total_length = sum([tv[w]['tf']*len(w) for w in tv])
            result.append({'id': id, 'avg_word_length': total_length/word_count})
        return pd.DataFrame(result)

    def resolve(self, solr_results, view, feature_map):
        if view not in self.schema_map:
            print("Error: view {} not in schema".format(view))
            exit(1)

        maps = self.schema_map[view]
        funcs = {}
        for f in feature_map.keys():
            func = maps[f]
            if func not in funcs: funcs[func] = []
            funcs[func].append(f)

        df_result = None
        for func,features in funcs.items():
            result = func(solr_results)
            if result is None: continue
            #print(result.columns)
            if df_result is None: df_result = result[features+['id']]
            else: df_result = df_result.merge(result[features+['id']], on='id')
        # map from src_col to dst_col
        df_result = df_result.rename(columns=feature_map)
        return df_result

    def extract_term_vectors(self, term_list, field):
        if not term_list: return None
        if self.term_vectors: return self.term_vectors
        docs = list2dict(term_list)
        term_vectors = {}
        for docid in docs:
            doc_terms = list2dict(docs[docid])
            tvlist = list2dict(doc_terms[field])
            tv = {f: list2dict(tvlist[f]) for f in tvlist}
            term_vectors[docid] = tv
        self.term_vectors = term_vectors
        return self.term_vectors

    def construct_view(self, results):
        #return tvc
        return None

