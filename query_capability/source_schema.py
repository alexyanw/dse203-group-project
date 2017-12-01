import sys

class SourceTable:
    schema = {
        'customers': ['customerid', 'householdid', 'gender', 'firstname'],
        'products': ['productid', 'name', 'groupcode', 'groupname', 'isinstock', 'fullprice', 'asin', 'nodeid'],
        'orders': ['orderid', 'customerid', 'campaignid', 'orderdate', 'city', 'state', 'zipcode', 'paymenttype', 'totalprice', 'numorderlines', 'numunits'],
        'orderlines': ['orderlineid', 'orderid', 'productid', 'shipdate', 'billdate', 'unitprice', 'numunits', 'totalprice'],
        'campaigns': ['campaignid' 'campaignname', 'channel', 'discount', 'freeshippingflag'],
        'calendar': ['date', 'iso', 'datenum', 'dow', 'dowint', 'year', 'month', 'dom', 'monthabbr', 'doy', 'mondays', 'tuesdays', 'wednesdays', 'thursdays', 'fridays', 'saturdays', 'sundays', 'numholidays', 'holidayname', 'holidaytype', 'hol_national', 'hol_minor', 'hol_christian', 'hol_jewish', 'hol_muslim', 'hol_chinese', 'hol_other'],
        'reviews': ['reviewid', 'asin', 'reviewername', 'helpful', 'unixreviewtime', 'reviewtext', 'overall', 'reviewtime', 'summary'],
        'subscribers': ['subscriberid', 'rateplan', 'monthlyfee', 'market', 'channel', 'startdate', 'stopdate', 'stoptype', 'tenure', 'isactive'],
        'zipcounty': ['zipcode', 'latitude', 'longitude', 'poname', 'zipclass', 'countyfips', 'state', 'countyname', 'countypop', 'countyhu', 'countylandareamiles', 'countywaterareamiles'],
    }

    def __init__(self, table):
        self.table = table

    def _execute(self, cmd, **kwargs):
        dbcmd = kwargs.get('prefix', '')
        dbcmd += cmd
        if 'limit' in kwargs:
            dbcmd += "LIMIT {}".format(kwargs['limit'])
        return dbcmd

    @classmethod
    def getColumn(cls, table, idx):
        if table not in cls.schema:
            print("Required table '{}' doesn't exist\n".format(table))
            exit(1)
        return cls.schema[table][idx]

    # uniform interface
    def get_views(self, features=[], **kwargs):
        if len(features) == 0:
            features = '*'

        dbcmd = '''
SELECT {}
FROM {}
'''.format(', '.join(features), self.table)
        
        if 'where' in kwargs:
            dbcmd += 'WHERE' + "AND {}\n".format(kwargs['where'])

        return [self._execute(dbcmd, **kwargs)]

