import psycopg2
import sys
import pysolr
from util.asterixdb_python import *
import os
import pickle
import datetime
from hashlib import sha1

class Cacheable:
    def __init__(self):
        self._dir = '.cache'
        self._ttl = 60

    def _clear_cache(self):
        for the_file in os.listdir(self._dir):
            file_path = os.path.join(self._dir, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)

    def _search_cache(self, query):
        query_path = os.path.join(self._dir, str(sha1(query).hexdigest()))

        if not os.path.isfile(query_path):
            return None

        cached = pickle.load(open(query_path, 'rb'))

        if cached['timestamp'] + datetime.timedelta(0, 0, self._ttl) >= datetime.datetime.now():
            os.remove(query_path)
            return None

        return cached['response']

    def _cache(self, query, response):
        query_path = os.path.join(self._dir, str(sha1(query).hexdigest()))

        if os.path.isfile(query_path):
            os.remove(query_path)

        cache_obj = {
            'timestamp': datetime.datetime.now(),
            'response': response
        }

        pickle.dump(cache_obj, open(query_path, 'wb'))

class AsterixSource:
    def __init__(self, server_url):
        self._asterix_conn = psycopg2.connect(connection_string)

    def _execSqlPpQuery(self,query, params=None):
        c = self._postgres_conn.cursor()

        c.execute(query,params)
        results = c.fetchall()
        columns = [desc[0] for desc in c.description]

        c.close()
        return SqlQueryResponse(
            columns=columns,
            results=results
        )

class SolrSource(Cacheable):
    def __init__(self, solr_config):
        Cacheable.__init__(self)
        self._solr_conn = solr = pysolr.Solr(solr_config.host, timeout=10)

    def _execSolrQuery(self, q, **kwargs):
        return self._solr_conn.search(
            q=q,
            search_handler='select',
            **kwargs)

class SqlQueryResponse:
    def __init__(self, columns, results):
        self.columns = columns
        self.results = results


class SqlSource(Cacheable):
    def __init__(self, sql_config):
        Cacheable.__init__(self)
        try:
            self._postgres_conn = psycopg2.connect(sql_config.connection_string)
        except psycopg2.OperationalError as e:
            print('Unable to connect!\n{0}').format(e)
            sys.exit(1)

        self._random_seed = sql_config.random_seed

    def __del__(self):
        if self._postgres_conn:
            self._postgres_conn.close()

    def _execSqlQuery(self,query, params=None):
        c = self._postgres_conn.cursor()

        compiled_query = c.mogrify(query,params)
        cache_hit = self._search_cache(compiled_query)

        if cache_hit:
            print('cache hit')
            response = cache_hit
        else:
            print('cache miss')
            c.execute(query, params)
            results = c.fetchall()
            columns = [desc[0] for desc in c.description]

            response = SqlQueryResponse(
                columns=columns,
                results=results
            )

            self._cache(c.query, response)

        c.close()

        return response

