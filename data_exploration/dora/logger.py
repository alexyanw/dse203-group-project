import os
from datetime import datetime
from functools import wraps
from uuid import getnode as get_mac
from .benchmarks import Benchmarks
from .config import Config

def log(f):
    """Decorator function to log API function calls to local log file and to SqlSource."""

    _log_file = '.log'

    if not os.path.isfile(_log_file):
        l = open(_log_file, "w")
        l.close()

    sql_config = Config().sql_config
    benchmarks = Benchmarks(sql_config)
    mac = get_mac()

    @wraps(f)
    def wrap(self, *args, **kwargs):

        function = f.__qualname__
        args_str = str(args)
        kwargs_str = str(kwargs)
        start = str(datetime.now())

        result = f(self, *args, **kwargs)

        is_cached = str(result.is_cached if hasattr(result, 'is_cached') else False)
        end = str(datetime.now())

        with open(_log_file, "a") as l:
            l.write(
                function+ ',' +
                args_str+ ',' +
                kwargs_str+ ',' +
                start+ ',' +
                end+ ',' +
                is_cached +
                '\n')

        benchmarks.insert(
            function,
            args_str,
            kwargs_str,
            start,
            end,
            is_cached,
            mac
        )

        return result

    return wrap