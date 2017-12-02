import psycopg2
import sys
import pysolr
import os
import pickle
import datetime
from hashlib import sha1
from urllib import parse, request
from json import loads

class QueryResponse(object):
    def __init__(self, columns, results):
        self.columns = columns
        self.results = results
        self.is_cached = False

class Cacheable(object):
    def __init__(self, ttl):
        self._dir = '.cache'
        self._ttl = ttl

        if not os.path.exists(self._dir):
            os.makedirs(self._dir)

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

        if (
            ('timestamp' not in cached.keys())
            or ('response' not in cached.keys())
            or (cached['timestamp'] + datetime.timedelta(0, 0, self._ttl) >= datetime.datetime.now())
        ):
            os.remove(query_path)
            return None

        cached['response'].is_cached = True
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

class AsterixQueryResponse:
    def __init__(self, raw_response):
        self._json = loads(raw_response)

        self.requestID = self._json['requestID'] if 'requestID' in self._json else None
        self.clientContextID = self._json['clientContextID'] if 'clientContextID' in self._json else None
        self.signature = self._json['signature'] if 'signature' in self._json else None
        self.results = self._json['results'] if 'results' in self. _json else None
        self.metrics = self._json['metrics'] if 'metrics' in self._json else None

class AsterixConnection:
    def __init__(self, server):
        self._server = server

    def query(self, statement, pretty=False, client_context_id=None):
        endpoint = '/query/service'

        url = self._server + endpoint


        payload = {
            'statement': statement,
            'pretty': pretty
        }

        if client_context_id:
            payload['client_context_id'] = client_context_id

        data = parse.urlencode(payload).encode("utf-8")
        req = request.Request(url, data)
        response = request.urlopen(req).read()

        return AsterixQueryResponse(response)

class AsterixSource(Cacheable):
    def __init__(self, asterix_config):
        Cacheable.__init__(self, asterix_config.cache_ttl)
        self._asterix_conn = AsterixConnection(asterix_config.host)
        self._collection = asterix_config.collection

    def _transform_server_response(self, server_response):
        results = server_response.results

        if (results == None) | (len(results) == 0):
            return QueryResponse(
            columns=[],
            results=[])

        fields = [str(x) for x in results[0].keys()]
        tuples = [tuple([dic[field] for field in fields]) for dic in results]

        return QueryResponse(
            columns=fields,
            results=tuples
        )

    def _compile_query(self, query, params=None):
        if params is not None:
            query = query.format(**params)

        compiled = 'use ' + self._collection + ';' + query
        return compiled.encode('utf-8')

    def _execSqlPpQuery(self, query, params=None):
        compiled_query = self._compile_query(query, params)
        cache_hit = self._search_cache(compiled_query)

        if cache_hit is not None:
            response = cache_hit
        else:
            server_response = self._asterix_conn.query(compiled_query)
            response = self._transform_server_response(server_response)
            self._cache(compiled_query, response)

        return response

class SolrSource(Cacheable):
    def __init__(self, solr_config):
        Cacheable.__init__(self, solr_config.cache_ttl)
        self._solr_conn = solr = pysolr.Solr(solr_config.host, timeout=10)

    def _transform_server_response(self, server_response, facet_field=None):
        if facet_field is not None:
            facet_results = server_response.facets['facet_fields'][facet_field]
            return QueryResponse(
                columns=[facet_field, 'score'],
                results=[
                    (facet_results[i - 1], x)
                    for i, x
                    in enumerate(facet_results)
                    if i % 2 > 0])

    def _compile_query(self, query, params=None):
        compiled_query = {'q': query}
        if params is not None:
            compiled_query.update(params)
        return str(compiled_query).encode('utf-8')

    def _get_response(self,compiled_query, params):
        facet_field = params['facet.field'] if 'facet' in params.keys() else None
        full_response = self._solr_conn.search(
            q=compiled_query,
            search_handler='select',
            **params)

        return self._transform_server_response(full_response, facet_field)

    def _execSolrQuery(self, query, params):
        compiled_query = self._compile_query(query, params)
        cache_hit = self._search_cache(compiled_query)

        if cache_hit is not None:
            response = cache_hit
        else:
            response = self._get_response(query, params)
            self._cache(compiled_query, response)

        return response

class SqlSource(Cacheable):
    def __init__(self, sql_config):
        Cacheable.__init__(self, sql_config.cache_ttl)
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
            response = cache_hit
        else:
            c.execute(query, params)
            results = c.fetchall()
            columns = [desc[0] for desc in c.description]

            response = QueryResponse(
                columns=columns,
                results=results
            )

            self._cache(c.query, response)

        c.close()

        return response

