from datasources import AsterixSource
from logger import log

class Categories(AsterixSource):

    @log
    def search(self, stringsearch = '', classfication_only=False):
        fields = '''user.classification,
                    user.nodeID AS node_id,
                    user.category.level_0 AS level_0,
                    user.category.nested.level_1 AS level_1,
                    user.category.nested.nested.level_2 AS level_2,
                    user.category.nested.nested.nested.level_3 AS level_3,
                    user.category.nested.nested.nested.nested.level_4 AS level_4,
                    user.category.nested.nested.nested.nested.nested.level_5 AS level_5'''

        if classfication_only:
            query = '''
                SELECT '''+ fields + '''
                FROM ClassificationInfo user
                WHERE user.classification LIKE "%{search}%"'''
        else:
            query = '''
                SELECT '''+ fields + '''
                FROM ClassificationInfo user
                WHERE user.classification LIKE "%{search}%"
                    or user.category.level_0 LIKE "%{search}%"
                    or user.category.nested.level_1 LIKE "%{search}%"
                    or user.category.nested.nested.level_2 LIKE "%{search}%"
                    or user.category.nested.nested.nested.level_3 LIKE "%{search}%"
                    or user.category.nested.nested.nested.nested.level_4 LIKE "%{search}%"
                    or user.category.nested.nested.nested.nested.nested.level_5 LIKE "%{search}%";'''

        return self._execSqlPpQuery(query, {'search':stringsearch})
    
    
        
        def parentOf_depth1(self, nodeID = 0, depth = None):
        return self.query('''
        use TinySocial;
        SELECT CASE WHEN ci.category.nested.nested.level_2 = "N/A" THEN
                    (SELECT VALUE c.nodeID
                    FROM ClassificationInfo c
                    WHERE ci.category.level_0 = c.category.level_0 
                        and c.category.nested.level_1 = "N/A")
                WHEN ci.category.nested.nested.nested.level_3 = "N/A" THEN
                    (SELECT VALUE c.nodeID
                    FROM ClassificationInfo c
                    WHERE ci.category.level_0 = c.category.level_0
                        and ci.category.nested.level_1 = c.category.nested.level_1
                        and c.category.nested.nested.level_2 = "N/A")
                WHEN ci.category.nested.nested.nested.nested.level_4 = "N/A" THEN
                    (SELECT VALUE c.nodeID
                    FROM ClassificationInfo c
                    WHERE ci.category.level_0 = c.category.level_0
                        and ci.category.nested.level_1 = c.category.nested.level_1
                        and ci.category.nested.nested.level_2 = c.category.nested.nested.level_2
                        and c.category.nested.nested.nested.level_3 = "N/A")
                WHEN ci.category.nested.nested.nested.nested.nested.level_5 = "N/A" THEN
                    (SELECT VALUE c.nodeID
                    FROM ClassificationInfo c
                    WHERE ci.category.level_0 = c.category.level_0
                        and ci.category.nested.level_1 = c.category.nested.level_1
                        and ci.category.nested.nested.level_2 = c.category.nested.nested.level_2
                        and ci.category.nested.nested.nested.level_3 = c.category.nested.nested.nested.level_3
                        and c.category.nested.nested.nested.nested.level_4 = "N/A")                
                ELSE 
                    (SELECT VALUE c.nodeID
                    FROM ClassificationInfo c
                    WHERE ci.category.level_0 = c.category.level_0 
                        and ci.category.nested.level_1 = c.category.nested.level_1 
                        and ci.category.nested.nested.level_2 = c.category.nested.nested.level_2
                        and ci.category.nested.nested.nested.level_3 = c.category.nested.nested.nested.level_3
                        and ci.category.nested.nested.nested.nested.level_4 = c.category.nested.nested.nested.nested.level_4
                        and c.category.nested.nested.nested.nested.nested.level_5 = "N/A")  
                END AS parents
        FROM ClassificationInfo ci
        WHERE ci.nodeID = {};'''.format(nodeID))
