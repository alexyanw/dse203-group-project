import sys

class SourceTable:
    def __init__(self, table):
        self.table = table
        self.schema_map = {
        }

    def _execute(self, cmd, **kwargs):
        dbcmd = kwargs.get('prefix', '')
        dbcmd += cmd
        if 'limit' in kwargs:
            dbcmd += "LIMIT {}".format(kwargs['limit'])
        return dbcmd

    # uniform interface
    def get_views(self, features=[], **kwargs):
        if len(features) == 0:
            features = '*'
        # validate features against schema_map

        dbcmd = '''
SELECT {}
FROM {}
'''.format(', '.join(features), self.table)
        
        if 'where' in kwargs:
            dbcmd += 'WHERE' + "AND {}\n".format(kwargs['where'])

        return [self._execute(dbcmd, **kwargs)]

