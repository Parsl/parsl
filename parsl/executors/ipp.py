import os
import logging
from ipyparallel import Client
from parsl.executors.base import ParslExecutor
from parsl.executors.errors import *

logger = logging.getLogger(__name__)

class IPyParallelExecutor(ParslExecutor):
    ''' The Ipython parallel executor.
    This executor allows us to take advantage of multiple processes running locally
    or remotely via  IPythonParallel's pilot execution system.

    .. note:: Some deficiencies with this executor are:
             1. Ipengine's execute one task at a time. This means one engine per core
    is necessary to exploit the full parallelism of a node.
             2. No notion of remaining walltime.
             3. Lack of throttling means tasks could be queued up on a worker.

    '''


    def compose_launch_cmd(self, filepath, engine_dir):
        ''' Reads the json contents from filepath and uses that to compose the engine launch command

        Args:
            filepath: Path to the engine file
            engine_dir : CWD for the engines .
        '''

        self.engine_file = os.path.expanduser(filepath)

        engine_json = None
        try:
            with open (self.engine_file, 'r') as f:
                engine_json = f.read()

        except OSError as e:
            logger.error("Could not open engine_json : ", self.engine_file)
            raise e

        return '''cd {0}
cat <<EOF > ipengine.json
{1}
EOF

mkdir -p '.ipengine_logs'
ipengine --file=ipengine.json &>> .ipengine_logs/$jobname.log
'''.format(engine_dir, engine_json)


    def __init__ (self, execution_provider=None,
                  reuse_controller=True,
                  engine_json_file='~/.ipython/profile_default/security/ipcontroller-engine.json',
                  engine_dir='.',
                  config = None):
        ''' Initialize the IPyParallel pool

        Args:
             - self

        KWargs:
             - execution_provider (ExecutionProvider object)
             - reuse_controller (Bool) : If True ipp executor will attempt to connect to an available
               controller.
        '''

        self.executor = Client()
        self.launch_cmd = self.compose_launch_cmd(engine_json_file, engine_dir)
        self.config = config

        self.execution_provider = execution_provider
        self.engines = []

        if reuse_controller:
            # Reuse existing controller if one is available
            pass

        if execution_provider:
            self._scaling_enabled = True
            logger.debug("Starting IpyParallelExecutor with provider:%s", execution_provider)
            try:
                logger.debug("Attempting scale out -----------------")
                for i in range(self.config["execution"]["block"].get("initBlocks", 1)):
                    eng = self.execution_provider.submit(self.launch_cmd, 1)
                    logger.debug("Launched block : {0}:{1}".format(i, eng))
                    if not eng:
                        raise(ScalingFailed(self.execution_provider.sitename, 
                                            "Ipp executor failed to scale via execution_provider"))
                    self.engines.extend([eng])
                logger.debug("scale out done-----------------")

            except Exception as e:
                logger.error("Scaling out failed : %s", e)
                raise e

        else:
            self._scaling_enabled = False
            logger.debug("Starting IpyParallelExecutor with no provider")



        self.lb_view  = self.executor.load_balanced_view()
        logger.debug("Starting executor")


    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def submit (self,  *args, **kwargs):
        ''' Submits work to the thread pool
        This method is simply pass through and behaves like a submit call as described
        here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        Returns:
              Future
        '''
        logger.debug("Got args : %s,", args)
        logger.debug("Got kwargs : %s,", kwargs)
        return self.lb_view.apply_async(*args, **kwargs)

    def scale_out (self, *args, **kwargs):
        ''' Scales out the number of active workers by 1
        This method is notImplemented for threads and will raise the error if called.

        Raises:
             NotImplemented exception
        '''
        if self.execution_provider :
            r = self.execution_provider.submit(self.launch_cmd, *args, **kwargs)
        else:
            logger.error("No execution provider available")
            r = None

        return r

    def scale_in (self, *args, **kwargs):
        ''' Scale in the number of active workers by 1
        This method is notImplemented for threads and will raise the error if called.

        Raises:
             NotImplemented exception
        '''
        if self.execution_provider :
            r = self.execution_provider.cancel(*args, **kwargs)
        else:
            logger.error("No execution provider available")
            r = None

        return r


if __name__ == "__main__" :

    pool1_config = {"poolname" : "pool1",
                    "queue"    : "foo" }
