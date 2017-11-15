import sys

class CustomerView:
    def __init__(self):
        self.schema_map = {
        }

    def _execute(self, viewname, cmd, **kwargs):
        dbcmd = kwargs.get('prefix', '')
        if 'view' in kwargs:
            dbcmd += "{} as ({})".format(viewname, cmd)
            return dbcmd
        dbcmd += cmd
        if 'limit' in kwargs:
            dbcmd += "LIMIT {}".format(kwargs['limit'])
        return dbcmd

    def get_customer_view(self, **kwargs):
        cmd = '''
SELECT c.customerid, c.gender, c.firstname, count(1) as total_order_count
FROM customers c, orders o
WHERE c.customerid = o.customerid
GROUP BY p.customerid
'''
        return self._execute('customer_view', cmd, **kwargs)

    # uniform interface
    def get_views(self, features=[], **kwargs):
        return self.get_customer_view(**kwargs)

        
