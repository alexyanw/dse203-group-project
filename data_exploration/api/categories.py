from util.asterixdb_python import AsterixConnection

class Categories(AsterixConnection):

    def search(self, stringsearch = ''):
        return self.query('''
        use TinySocial;
        SELECT VALUE user
        FROM ClassificationInfo user
        WHERE user.category.level_0 LIKE "%{}%"'''.format(stringsearch)
            +'''or user.category.nested.level_1 LIKE "%{}%"'''.format(stringsearch)
            +'''or user.category.nested.nested.level_2 LIKE "%{}%"'''.format(stringsearch)
            +'''or user.category.nested.nested.nested.level_3 LIKE "%{}%"'''.format(stringsearch)
            +'''or user.category.nested.nested.nested.nested.level_4 LIKE "%{}%"'''.format(stringsearch)
            +'''or user.category.nested.nested.nested.nested.nested.level_5 LIKE "%{}%";'''.format(stringsearch))
