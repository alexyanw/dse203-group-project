import sys
import re
import logging, pprint
from utils import *

__all__ = ['DatalogParserFE']

logger = logging.getLogger('qe.DatalogParserFE')

class DatalogParserFE:
    def __init__(self):
        #logger.debug("parser structure:\n{}".format(pprint.pformat(self.__dict__)))
        None

    def parse(self, datalog):
        subqueries = self.breakquery(datalog)
        result= [self.atomize(q) for q in subqueries]
        #logger.info("datalog decomposed:\n{}\n".format(pprint.pformat(result)))
        return result

    def breakquery(self, datalog):
        subqueries = []
        fmtdl = re.sub("<-", "<-\n", datalog)
        if re.search('setof', fmtdl):
            breakset = fmtdl.index('setof')
            fmtdl = re.sub("\)\s*,", ")\n", fmtdl[:breakset]) + fmtdl[breakset:]
        else:
            fmtdl = re.sub("\)\s*,", "),\n", fmtdl)
        idx = 0
        for stmt in fmtdl.split('\n'):
            stmt = re.sub("^\s+", '', stmt)
            stmt = re.sub("\s+$", '', stmt)
            if stmt == '': continue
            if re.search('<-', stmt):
                subqueries.append([stmt])
            else:
                if re.search('\(', stmt):
                    subqueries[-1].append(stmt)
                elif re.search('setof', stmt):
                    subqueries[-1].append(stmt)
                else:
                    subqueries[-1] += stmt.split(',')
        return subqueries

    def atomize(self, subdatalog):
        struct = {'result': None, 'table':[], 'condition':[]}
        for atom in subdatalog:
            if re.search('^\s*$', atom): continue
            if re.search('<-', atom):
                struct['result'] = re.sub('\s*<-\s*', '', atom)
            elif re.search('[<=>]', atom):
                struct['condition'] += atom.split(',')
            elif re.search('setof', atom):
                struct['groupby'] = self._resolve_groupby(atom)
                struct['table'].append(struct['groupby']['table'])
                struct['groupby'].pop('table', None)
            elif re.search('orderby', atom):
                struct['orderby'] = re.sub(".*orderby\s+", '', atom)
            elif re.search('limit', atom):
                struct['limit'] = re.sub(".*limit\s+", '', atom)
            else:
                struct['table'].append(re.sub("\),", ')', atom))
        return struct

    def _resolve_groupby(self, atom):
        ''' eg:
        q(X, A) :- setof({Z}, {Y}, p(X, Y, Z), S), count(S, A)
        q(product_a, A) :- setof({pair_count}, E^b. CoMat(product_a, b, pair_count), S), sum(S, A)'''
        atom = re.sub('\s+', '', atom)
        aggs = []
        match = re.search('setof\(([^\(]*\([^\(\)]*\)[^\)]*)\),(.*)$', atom)
        if not match:
            fatal('invalid datalog group: {}'.format(atom))
        str_setof,str_agg = match.group(1),match.group(2)
        aggs = re.sub("\),", ")\n", str_agg).split("\n")

        match = re.search('(.*),(.*\(.*\)),()', str_setof)
        if not match:
            fatal('invalid datalog setof: {}'.format(str_setof))
        free,relation,str_set = match.group(1),match.group(2),match.group(3)
        free_vars = []
        for var in free.split(','):
            match = re.search('{(.*)}', var)
            if not match:
                fatal('invalid datalog free variable: {}'.format(var))
            free_vars.append(match.group(1))
        str_columns = relation[relation.index('(')+1:-1]
        columns = set(str_columns.split(','))

        groupkeys = columns - set(free_vars)
        return {'key': ','.join(groupkeys), 'aggregation': aggs, 'table': relation}



