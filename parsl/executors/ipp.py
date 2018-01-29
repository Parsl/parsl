import os
import time
import logging
from ipyparallel import Client

from parsl.executors.base import ParslExecutor
from parsl.executors.errors import *

logger = logging.getLogger(__name__)


class IPyParallelExecutor(ParslExecutor):
    ''' The Ipython parallel executor.
    This executor allows us to take advantage of multiple processes running locally
    or remotely via  IPythonParallel's pilot execution system.

    .. note::
           Some deficiencies with this executor are:

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
            with open(self.engine_file, 'r') as f:
                engine_json = f.read()

        except OSError as e:
            logger.error("Could not open engine_json : ", self.engine_file)
            raise e

        return '''cd {0}
cat <<EOF > ipengine.json
{1}
EOF

mkdir -p '.ipengine_logs'
ipengine --file=ipengine.json &>> .ipengine_logs/$JOBNAME.log
'''.format(engine_dir, engine_json)

    def __init__(self, execution_provider=None,
                 reuse_controller=True,
                 engine_json_file='~/.ipython/profile_default/security/ipcontroller-engine.json',
                 engine_dir='.',
                 controller=None,
                 config=None):
        ''' Initialize the IPyParallel pool. The initialization takes all relevant parameters via KWargs.

        .. note::

              If initBlocks > 0, and a scalable execution_provider is attached, then the provider
              will be initialized here.

        Args:
             - self

        KWargs:
             - execution_provider (ExecutionProvider object)
             - reuse_controller (Bool) : If True ipp executor will attempt to connect to an available
               controller. Default: True
             - engine_json_file (str): Path to json engine file that will be used to compose ipp launch
               commands at scaling events. Default : '~/.ipython/profile_default/security/ipcontroller-engine.json'
             - engine_dir (str) : Alternative to above, specify the engine_dir
             - config (dict). Default: '.'
        '''
        self.controller = controller
        self.engine_file = engine_json_file
        self.client_file = None

        if self.controller:
            # Find the Client json
            self.client_file = self.controller.client_file
            self.engine_file = self.controller.engine_file

            if not os.path.exists(self.client_file):
                logger.debug("Waiting for {0}".format(self.client_file))

            sleep_dur = 20  # 20 seconds
            for i in range(0, int(sleep_dur / 0.2)):
                time.sleep(0.2)
                if os.path.exists(self.client_file):
                    break

            if not os.path.exists(self.client_file):
                raise Exception("Controller client file is missing at {0}".format(self.client_file))

        self.executor = Client(url_file=self.client_file)
        self.config = config
        self.sitename = config['site'] if config else 'Static_IPP'
        # NOTE: Copying the config here only partially fixes the issue. There needs to be
        # multiple controllers launched by the factory, and each must have different jsons.
        # There could be timing issues here,
        # local_engine_json = "{0}.{1}.engine.json".format(self.config["site"], int(time.time()))
        # copyfile(engine_json_file, local_engine_json)
        # if not os.path.exists(self.config["execution"]["script_dir"]):
        #    os.makedirs(self.config["execution"]["script_dir"])

        self.launch_cmd = self.compose_launch_cmd(self.engine_file, engine_dir)
        self.execution_provider = execution_provider
        self.engines = []

        if reuse_controller:
            # Reuse existing controller if one is available
            pass

        if execution_provider:
            self._scaling_enabled = True
            logger.debug("Starting IpyParallelExecutor with provider:%s", execution_provider)
            try:
                for i in range(self.config["execution"]["block"].get("initBlocks", 1)):
                    eng = self.execution_provider.submit(self.launch_cmd, 1)
                    logger.debug("Launched block : {0}:{1}".format(i, eng))
                    if not eng:
                        raise(ScalingFailed(self.execution_provider.sitename,
                                            "Ipp executor failed to scale via execution_provider"))
                    self.engines.extend([eng])

            except Exception as e:
                logger.error("Scaling out failed : %s", e)
                raise e

        else:
            self._scaling_enabled = False
            logger.debug("Starting IpyParallelExecutor with no provider")

        self.lb_view = self.executor.load_balanced_view()
        logger.debug("Starting executor")

    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def submit(self, *args, **kwargs):
        ''' Submits work to the thread pool
        This method is simply pass through and behaves like a submit call as described
        here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        Returns:
              Future
        '''
        logger.debug("Got args : %s,", args)
        logger.debug("Got kwargs : %s,", kwargs)
        return self.lb_view.apply_async(*args, **kwargs)

    def scale_out(self, *args, **kwargs):
        ''' Scales out the number of active workers by 1
        This method is notImplemented for threads and will raise the error if called.

        '''
        if self.execution_provider:
            r = self.execution_provider.submit(self.launch_cmd, *args, **kwargs)
            self.engines.extend([r])
        else:
            logger.error("No execution provider available")
            r = None

        return r

    def scale_in(self, blocks, *args, **kwargs):
        ''' Scale in the number of active workers by 1
        This method is notImplemented for threads and will raise the error if called.

        Raises:
             NotImplemented exception
        '''
        status = dict(zip(self.engines, self.execution_provider.status(self.engines)))

        # This works for blocks=0
        to_kill = [engine for engine in status if status[engine] == "RUNNING"][:blocks]

        if self.execution_provider:
            r = self.execution_provider.cancel(to_kill, *args, **kwargs)
        else:
            logger.error("No execution provider available")
            r = None

        return r

    def status(self):
        ''' Returns the status of the executor via probing the execution providers.

        '''
        if self.execution_provider:
            status = self.execution_provider.status(self.engines)

        else:
            status = []

        return status

    def shutdown(self, hub=True, targets='all', block=False):
        ''' Shutdown the executor, including all workers and controllers.
        The interface documentation for IPP is `here <http://ipyparallel.readthedocs.io/en/latest/api/ipyparallel.html#ipyparallel.Client.shutdown>`_

        Kwargs:
            - hub (Bool): Whether the hub should be shutdown, Default:True,
            - targets (list of ints| 'all'): List of engine id's to kill, Default:'all'
            - block (Bool): To block for confirmations or not

        Raises:
             NotImplemented exception
        '''

        if self.controller:
            logger.debug("IPP:Shutdown sequence: Attempting controller kill")
            self.controller.close()

        # We do not actually do executor.shutdown because
        # this blocks even when requested to not block, killing the
        # controller is more effective although impolite.
        # x = self.executor.shutdown(targets=targets,
        #                           hub=hub,
        #                           block=block)

        logger.debug("Done with executor shutdown")
        return True

    def __repr__(self):
        return "<IPP Executor for site:{0}>".format(self.sitename)


if __name__ == "__main__":

    pool1_config = {"poolname": "pool1",
                    "queue": "foo"}
