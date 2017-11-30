import os
from datetime import datetime
from functools import wraps

def log(f):
    _log_file = '.log'

    if not os.path.isfile(_log_file):
        l = open(_log_file, "w")
        l.close()

    @wraps(f)
    def wrap(self, *args, **kwargs):
        line = f.__qualname__ + ',' + str(args) + ',' + str(kwargs) + ',' + str(datetime.now())
        result = f(self, *args, **kwargs)
        is_cached = str(result.is_cached if hasattr(result, 'is_cached') else False)

        line += ',' + str(datetime.now()) + ',' + is_cached + '\n'
        with open(_log_file, "a") as l:
            l.write(line)
        return result

    return wrap