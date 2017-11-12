import psycopg2
import sys
import pysolr
from util.asterixdb_python import *

class AsterixDbSource:
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

class SolrSource:
    def __init__(self, solr_config):
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


class SqlSource:
    def __init__(self, sql_config):
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

        c.execute(query,params)
        results = c.fetchall()
        columns = [desc[0] for desc in c.description]

        c.close()
        return SqlQueryResponse(
            columns=columns,
            results=results
        )