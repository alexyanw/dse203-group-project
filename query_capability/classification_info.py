# Wrapper of ClassificationInfo dataset
from asterixdb_python import AsterixConnection 

class ClassificationInfo:
    def __init__(self, server = 'http://localhost', port = 19002, stat=False):
        self.asterix_conn = AsterixConnection(server, port)
        self.init_stat = stat
        if stat:
            self.get_stat()

    def get_stat(self):
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
        response = self.asterix_conn.query(cmd)
        levels = response.results[0]
        self.levels = {}
        for l in levels:
            if levels[l][0] is not None:
                self.levels[l] = levels[l]
        return self.levels

    def print_level_stat(self):
        if self.init_stat == False:
            print("Please call get_stat() first\n")
            return
        level_counts = {}
        for l in levels:
            count = len(self.levels[l])
            level_counts[l] = count
            print(l,':', count)
        print("total:",sum(level_counts.values()))
        
    def get_flat_records(self):
        cmd = '''
use TinySocial;
select c.nodeID, 
c.category.nested.level_1 , 
c.category.nested.nested.level_2, c.category.nested.nested.nested.level_3,
c.category.nested.nested.nested.nested.level_4,
c.category.nested.nested.nested.nested.nested.level_5
from ClassificationInfo c 
'''
        return self.asterix_conn.query(cmd).results
    
    def get_nodes_by_level(self, levels, **kwargs):
        ''' return all the nodes that match the levels in their hierarchy'''
        condition = ' and '.join(["m.{} = \"{}\"".format(l, levels[l]) for l in levels])
        cmd = '''
use TinySocial;
With node_map AS (
select c.nodeID AS nodeid, 
c.category.nested.level_1 , 
c.category.nested.nested.level_2, c.category.nested.nested.nested.level_3,
c.category.nested.nested.nested.nested.level_4,
c.category.nested.nested.nested.nested.nested.level_5
from ClassificationInfo c 
)
'''
        if id in kwargs:  # return nodeid as list
            cmd += '''
select value m.nodeid
from node_map m
'''
        else:  # return structure with both nodeid and level info
            cmd += '''
select value m
from node_map m
'''
        cmd += "where {};".format(condition)
        #print(cmd)
        return self.asterix_conn.query(cmd).results
    
    def get_nodes_by_category(self, categories, **kwargs):
        ''' return all the nodes that satisfy the categories in their hierarchy'''
        condition = ' and '.join(["\"{}\" in m.level".format(cat) for cat in categories])
        cmd = '''
use TinySocial;
With  node_map AS (
select value {'nodeid': c.nodeID, 'level' : [
c.category.nested.level_1,
c.category.nested.nested.level_2, c.category.nested.nested.nested.level_3,
c.category.nested.nested.nested.nested.level_4,
c.category.nested.nested.nested.nested.nested.level_5]
}
from ClassificationInfo c 
)
'''
        if id in kwargs:  # return nodeid as list
            cmd += '''
select value m.nodeid
from node_map m
'''
        else:  # return structure with both nodeid and level info
            cmd += '''
select m.nodeid, m.level
from node_map m
'''
        cmd += "where {};".format(condition)
        #print(cmd)
        return self.asterix_conn.query(cmd).results
    
