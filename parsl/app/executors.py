import concurrent.futures
import logging

logger = logging.getLogger(__name__)

def ThreadPoolExecutor(max_workers=None, thread_name_prefix=''):
    return concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
                                                 #thread_name_prefix=thread_name_prefix)

def ProcessPoolExecutor(max_workers=None):
    return concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)

