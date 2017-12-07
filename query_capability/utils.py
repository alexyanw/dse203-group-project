import logging
logger = logging.getLogger('qe')
# stream handler
sh = logging.StreamHandler()
#sh.setLevel(level)
#if verbose:
sformatter = logging.Formatter('<%(name)s> %(message)s')
#else:
#    sformatter = logging.Formatter('[qe] %(message)s')
sh.setFormatter(sformatter)
logger.addHandler(sh)

def set_logger(level, verbose):
    logger.setLevel(level)

def fatal(msg):
    print("Error:", msg)
    exit(1)
