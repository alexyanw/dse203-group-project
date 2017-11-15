from asterix_engine import AsterixEngine

datalog1 = [
    'Ans(nodeid, level_1, level_2)',
    'CategoryLevel(nodeid, level_1, level_2, level_3, level_4, level_5)'
    ]

datalog1 = [
    'Ans(nodeid)',
    'CategoryLevel(nodeid, level_1, level_2, level_3, level_4, level_5)',
    ["level_1 = 'Education'"]
    ]

datalog = [
    'Ans(nodeid)',
    'CategoryFlat(nodeid, category)',
    ["CategoryFlat.category = 'Education'"]
    ]
engine = AsterixEngine()
sqlpp = engine.queryDatalog(datalog, debug=True)
print(sqlpp)
