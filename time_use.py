# encoding utf-8
# from test

import time
import logging

def time_use(func):
    def newFunc(*args, **args2):
        t0 = time.time()
        logging.debug("@%s, {%s} start" % (time.strftime("%X", time.localtime()), func.__name__))
        back = func(*args, **args2)
        logging.debug("@%s, {%s} end" % (time.strftime("%X", time.localtime()), func.__name__))
        logging.debug("@%.3fs taken for {%s}" % (time.time() - t0, func.__name__))
        return back

    return newFunc
