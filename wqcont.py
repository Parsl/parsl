# test what happens when there are tasks outstanding at exit time

# this *should* cause parsl to shut down properly including the process exiting

import parsl
import time

parsl.set_stream_logger()

@parsl.python_app
def longlongapp(t):
    import time
    time.sleep(t)


#from parsl.tests.configs.htex_local_alternate import config
#from parsl.tests.configs.workqueue_ex import config
from parsl.tests.configs.workqueue_monitoring import config

#parsl.load()
parsl.load(config)

fut = longlongapp(3600)

time.sleep(10)


