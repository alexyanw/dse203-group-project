from .datasources import AsterixSource
from .logger import log

class Categories(AsterixSource):

    def __init__(self, asterix_config):
        AsterixSource.__init__(self,asterix_config)
        self._fields = '''
            ci.classification,
            ci.nodeID AS node_id,
            ci.category.level_0 AS level_0,
            ci.category.nested.level_1 AS level_1,
            ci.category.nested.nested.level_2 AS level_2,
            ci.category.nested.nested.nested.level_3 AS level_3,
            ci.category.nested.nested.nested.nested.level_4 AS level_4,
            ci.category.nested.nested.nested.nested.nested.level_5 AS level_5'''

    @log
    def stringsearch(self, string_search = ''):
        """Case-sensitive search of all category levels.

        Args:
            string_search (str)

        Returns:
            QueryResponse:
                columns (:obj:`list` of :obj:`str`): ['classification','nodeid','level_0','level_1','level_2','level_3','level_4','level_5']

                results (:obj:`list` of :obj:`tuple(str,int,str,str,str,str,str,str)`
        """
        query = '''
            SELECT '''+ self._fields + '''
            FROM ClassificationInfo ci
            WHERE ci.category.level_0 LIKE "%{search}%"
                or ci.category.nested.level_1 LIKE "%{search}%"
                or ci.category.nested.nested.level_2 LIKE "%{search}%"
                or ci.category.nested.nested.nested.level_3 LIKE "%{search}%"
                or ci.category.nested.nested.nested.nested.level_4 LIKE "%{search}%"
                or ci.category.nested.nested.nested.nested.nested.level_5 LIKE "%{search}%";'''

        return self._execSqlPpQuery(query, {'search':string_search})
    
    @log
    def nodesearch(self, node_id):
        """Numeric search by node_id of all category levels.

        Args:
            string_search (str)

        Returns:
            QueryResponse:
                columns (:obj:`list` of :obj:`str`): ['classification','nodeid','level_0','level_1','level_2','level_3','level_4','level_5']

                results (:obj:`list` of :obj:`tuple(str,int,str,str,str,str,str,str)`
        """
        query = '''
            SELECT '''+ self._fields + '''
            FROM ClassificationInfo ci
            WHERE ci.nodeID = {search};'''

        return self._execSqlPpQuery(query, {'search':node_id})

    @log
    def parentOf(self, node_id):
        """Get the direct parent category of node_id.

        Args:
            node_id (int)

        Returns:
            QueryResponse:
                columns (:obj:`list` of :obj:`str`): ['nodeID','level','parent_node_id']

                results (:obj:`list` of :obj:`tuple(int,int,int)`
                """

        node_id = str(node_id)

        levelquery = '''
                    SELECT
                    CASE
                        WHEN
                            ci.level_0 <> "N/A"
                            AND ci.level_1 = "N/A"
                        THEN 0
                        WHEN
                            ci.level_1 <> "N/A"
                            AND ci.level_2 = "N/A"
                        THEN 1
                        WHEN
                            ci.level_2 <> "N/A"
                            AND ci.level_3  = "N/A"
                        THEN 2
                        WHEN
                            ci.level_3  <> "N/A"
                            AND ci.level_4 = "N/A"
                        THEN 3
                        WHEN
                            ci.level_4 <> "N/A"
                            AND ci.level_5 = "N/A"
                        THEN 4
                        WHEN ci.level_5 <> "N/A"
                        THEN 5
                    END as level,
                    ci.level_0 AS level_0,
                    ci.level_1 AS level_1,
                    ci.level_2 AS level_2,
                    ci.level_3 AS level_3,
                    ci.level_4 AS level_4,
                    ci.level_5 AS level_5
                	FROM ClassificationInfo_Flattened ci
                	WHERE ci.node_id = {node_id}'''
        
        node_info = self._execSqlPpQuery(levelquery, {'node_id':node_id}).results
        lvl = node_info[0][0]
        level_0 = node_info[0][1]
        level_1 = node_info[0][2]
        level_2 = node_info[0][3]
        level_3 = node_info[0][4]
        level_4 = node_info[0][5] 
        
        query = '''SELECT {node_id} as nodeID, ''' + str(lvl) + ''' as level, ci.node_id as parent_node_id FROM ClassificationInfo_Flattened ci '''
            	
        if lvl == 1:
            query += '''WHERE ci.level_0 = "''' + level_0 + '''" AND ci.level_1 = "N/A";'''
        elif lvl == 2:
            query += '''WHERE
                ci.level_0 = "''' + level_0 + '''"
                AND ci.level_1 = "''' + level_1 + '''"
                AND ci.level_2 = "N/A";'''
        elif lvl == 3:
            query += '''WHERE
                ci.level_0 = "''' + level_0 + '''"
                AND ci.level_1 = "''' + level_1 + '''"
                AND ci.level_2 = "''' + level_2 + '''"
                AND ci.level_3 = "N/A";'''
        elif lvl == 4:
            query += '''WHERE
                ci.level_0 = "''' + level_0 + '''"
                AND ci.level_1 = "''' + level_1 + '''"
                AND ci.level_2 = "''' + level_2 + '''"
                AND ci.level_3 = "''' + level_3 + '''"
                AND ci.level_4 = "N/A";'''
        elif lvl == 5:
            query += '''WHERE
                ci.level_0 = "''' + level_0 + '''"
                AND ci.level_1 = "''' + level_1 + '''"
                AND ci.level_2 = "''' + level_2 + '''"
                AND ci.level_3 = "''' + level_3 + '''"
                AND ci.level_4 = "''' + level_4 + '''"
                AND ci.level_5 = "N/A";'''

        return self._execSqlPpQuery(query, {'node_id':node_id})
        
    @log
    def childrenOf(self, node_id):
        """Get the direct children categories of node_id.

        Args:
            node_id (int)

        Returns:
            QueryResponse:
                columns (:obj:`list` of :obj:`str`): ['nodeID','level','child_node_id']

                results (:obj:`list` of :obj:`tuple(int,int,int)`
                """
        levelquery = '''
                    SELECT
                    CASE
                        WHEN
                            ci.level_0 <> "N/A"
                            AND ci.level_1 = "N/A"
                        THEN 0
                        WHEN
                            ci.level_1 <> "N/A"
                            AND ci.level_2 = "N/A"
                        THEN 1
                        WHEN
                            ci.level_2 <> "N/A"
                            AND ci.level_3  = "N/A"
                        THEN 2
                        WHEN
                            ci.level_3  <> "N/A"
                            AND ci.level_4 = "N/A"
                        THEN 3
                        WHEN
                            ci.level_4 <> "N/A"
                            AND ci.level_5 = "N/A"
                        THEN 4
                        WHEN ci.level_5 <> "N/A"
                        THEN 5
                    END as level,
                    ci.level_0 AS level_0,
                    ci.level_1 AS level_1,
                    ci.level_2 AS level_2,
                    ci.level_3 AS level_3,
                    ci.level_4 AS level_4,
                    ci.level_5 AS level_5
                	FROM ClassificationInfo_Flattened ci
                	WHERE ci.node_id = {node_id}'''
        
        node_info = self._execSqlPpQuery(levelquery, {'node_id':node_id}).results
        lvl = node_info[0][0]
        level_0 = node_info[0][1]
        level_1 = node_info[0][2]
        level_2 = node_info[0][3]
        level_3 = node_info[0][4]
        level_4 = node_info[0][5] 
        
        query = '''SELECT {node_id} as nodeID, ''' + str(lvl) + ''' as level, ci.node_id as child_node_id FROM ClassificationInfo_Flattened ci '''
            	
        if lvl == 0:
        	query += '''WHERE
        	    ci.level_0 = "''' + level_0 + '''"
        	    AND ci.level_1 != "N/A"
        	    AND ci.level_2 = "N/A";'''
        elif lvl == 1:
            query += '''WHERE
                ci.level_0 = "''' + level_0 + '''"
                AND ci.level_1 = "''' + level_1 + '''"
                AND ci.level_2 != "N/A"
                AND ci.level_3 = "N/A";'''
        elif lvl == 2:
            query += '''WHERE
                ci.level_0 = "''' + level_0 + '''"
                AND ci.level_1 = "''' + level_1 + '''"
                AND ci.level_2 = "''' + level_2 + '''"
                AND ci.level_3 != "N/A"
                AND ci.level_4 = "N/A";'''
        elif lvl == 3:
            query += '''WHERE
                ci.level_0 = "''' + level_0 + '''"
                AND ci.level_1 = "''' + level_1 + '''"
                AND ci.level_2 = "''' + level_2 + '''"
                AND ci.level_3 = "''' + level_3 + '''"
                AND ci.level_4 != "N/A"
                AND ci.level_5 = "N/A";'''
        elif lvl == 4:
            query += '''WHERE
                ci.level_0 = "''' + level_0 + '''"
                AND ci.level_1 = "''' + level_1 + '''"
                AND ci.level_2 = "''' + level_2 + '''"
                AND ci.level_3 = "''' + level_3 + '''"
                AND ci.level_4 = "''' + level_4 + '''"
                AND ci.level_5 != "N/A";'''
            	
        return self._execSqlPpQuery(query, {'node_id':node_id})
