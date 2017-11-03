import subprocess
import time
import random
import logging

logger = logging.getLogger(__name__)

def init_controller(config):
    ''' Initialize ipython controllers to the user specified configs

    '''
    logger.debug("Starting ipcontroller")
    if config.get("controller", None):
        reuse = config["controller"].get("reuse", True)
        public_ip = config["controller"].get("publicIp", "*")
        rand_port = random.randint(50000, 60000)
        port = config["controller"].get("port", rand_port)
        proc = subprocess.Popen(['ipcontroller', 
                                 '--port={0}'.format(port),
                                 '--ip={0}'.format(public_ip)])        
        time.sleep(2)
        return proc
        
    else:
        return None


def shutdown_controller(proc_obj):
    try:
        proc.kill()
    except:
        logger.error("Failed to kill the ipcontroller process[{0}]".format(proc.pid))
    
    
