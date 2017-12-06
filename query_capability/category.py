import re
__all__ = ['Category']

class Category:
    schema = {
        'categorylevel' : ['nodeid', 'level_1', 'level_2', 'level_3', 'level_4', 'level_5'],
        'categoryflat' : ['nodeid', 'category'],
    }
    def __init__(self):
        self.schema_map = {
            'categorylevel' : (self.get_category_level, ('nodeid', 'level_1', 'level_2', 'level_3', 'level_4', 'level_5')),
            'categoryflat' : (self.get_category_flat, ('nodeid', 'category')),
        }

    def get_level_stat(self):
        cmd = '''
use TinySocial;
select (select distinct value c.category.nested.level_1 
from ClassificationInfo c ) as level_1,
(select distinct value c.category.nested.nested.level_2
from ClassificationInfo c ) as level_2,
(select distinct value c.category.nested.nested.nested.level_3
from ClassificationInfo c ) as level_3,
(select distinct value c.category.nested.nested.nested.nested.level_4
from ClassificationInfo c ) as level_4,
(select distinct value c.category.nested.nested.nested.nested.nested.level_5
from ClassificationInfo c ) as level_5,
(select distinct value c.category.nested.nested.nested.nested.nested.nested.level_6
from ClassificationInfo c ) as level_6,
(select distinct value c.category.nested.nested.nested.nested.nested.nested.nested.level_7
from ClassificationInfo c ) as level_7,
(select distinct value c.category.nested.nested.nested.nested.nested.nested.nested.nested.level_8
from ClassificationInfo c ) as level_8
;
'''
        return cmd

    def print_level_stat(self, results):
        levels = results[0]
        self.levels = {}
        for l in levels:
            if levels[l][0] is not None:
                self.levels[l] = levels[l]
        level_counts = {}
        for l in levels:
            count = len(self.levels[l])
            level_counts[l] = count
            print(l,':', count)
        print("total:",sum(level_counts.values()))

    def get_category_level(self, **kwargs):
        ''' return all the nodes that match the levels in their hierarchy'''
        dbcmd = '''
select c.nodeID AS nodeid, 
c.category.nested.level_1 , 
c.category.nested.nested.level_2, c.category.nested.nested.nested.level_3,
c.category.nested.nested.nested.nested.level_4,
c.category.nested.nested.nested.nested.nested.level_5
from ClassificationInfo c 
'''

        if 'view' in kwargs:
            dbcmd = "{} as ({})".format('categorylevel', dbcmd)
        return dbcmd
    
    def get_category_flat(self, **kwargs):
        ''' return all the nodes that satisfy the categories in their hierarchy'''
        dbcmd = '''
select value {'nodeid': c.nodeID, 'level' : [
c.category.nested.level_1,
c.category.nested.nested.level_2, c.category.nested.nested.nested.level_3,
c.category.nested.nested.nested.nested.level_4,
c.category.nested.nested.nested.nested.nested.level_5]
}
from ClassificationInfo c 
'''
        if 'view' in kwargs:
            dbcmd = "{} as ({})".format('categoryflat', dbcmd)
        return dbcmd
    
    def get_view(self, viewname, **kwargs):
        if viewname not in self.schema_map:
            print("Error: view {} not in schema".format(viewname))
            exit(1)

        maps = self.schema_map[viewname] 
        func = maps[0]
        view_content = func(**kwargs)
        return view_content

    @classmethod
    def getColumn(cls, table, idx):
        if table not in cls.schema:
            print("Required table '{}' doesn't exist\n".format(table))
            exit(1)
        return cls.schema[table][idx]

    @staticmethod
    def handleCategoryFlatCategory(column, relation, value):
        value = re.sub("'", '', value)
        categories = value.split(';')
        str_cond = ''
        if relation == '=':
            str_cond = ' AND '.join(["'{}' in categoryflat.level".format(cat) for cat in categories])
        elif relation == '!=': 
            str_cond = ' AND '.join(["'{}' not in categoryflat.level".format(cat) for cat in categories])
        elif relation == 'in':
            str_cond = ' OR '.join(["'{}' in categoryflat.level".format(cat) for cat in categories])

        return '(' + str_cond + ')'
