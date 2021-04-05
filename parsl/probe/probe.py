import parsl
import os
import sys
import shutil
import time
import copy
import subprocess
import zmq
import platform
import socket
import logging
import argparse
from importlib.machinery import SourceFileLoader

from parsl.launchers import SimpleLauncher
from parsl.executors import WorkQueueExecutor

verify_channel_file = '''
rand=$RANDOM
echo $rand > ./rand.txt
typeset -i readfromfile=$(cat ./rand.txt)
[[ $rand -eq $readfromfile ]]
'''

verify_channel_dir = '''
mkdir -p parent/child
rm -r parent
'''

verify_provider = '''
(
if [ -d ~/parsl_tmp ]; then
    rm -rf ~/parsl_tmp
fi

mkdir ~/parsl_tmp
echo "HOSTNAME=$HOSTNAME"                                                 > ~/parsl_tmp/env.txt
echo "WHICHPYTHON=$(which python)"                                       >> ~/parsl_tmp/env.txt
echo -n "PYTHON_VERSION="
python --version                                                         >> ~/parsl_tmp/env.txt
echo "PARSL_VERSION="`python -c 'import parsl;print(parsl.__version__)'` >> ~/parsl_tmp/env.txt
echo "PARSL_PATH="`python -c 'import parsl;print(parsl.__path__[0])'`    >> ~/parsl_tmp/env.txt
echo "ZMQ_VERSION="`python -c 'import zmq;print(zmq.zmq_version())'`     >> ~/parsl_tmp/env.txt
echo "PYZMQ_VERSION="`python -c 'import zmq;print(zmq.pyzmq_version())'` >> ~/parsl_tmp/env.txt
echo "PROC_WORKER_POOL="`which process_worker_pool.py`                   >> ~/parsl_tmp/env.txt
)
'''


def square(x):
    return x * x


class ConfigTester():

    def __init__(self, config):
        try:
            src = SourceFileLoader("config", config).load_module()
            config = src.config
        except Exception:
            raise
        self.config = config
        if os.path.isdir('~/parsl_tmp'):
            shutil.rmtree('~/parsl_tmp')

    def check_channel(self):
        logger.debug('\nBegin checking the channel(s) in the config')
        executors = self.config.executors
        logger.debug(f'There are {len(executors)} executors in the config')

        for i, executor in enumerate(executors):
            channels = None
            if hasattr(executor.provider, 'channel'):
                channels = [executor.provider.channel]
            else:
                channels = executor.provider.channels
            logger.debug(f'\tThere are {len(channels)} channels in #{i+1} executor')

            for j, channel in enumerate(channels):
                logger.debug(f'\t\tBegin checking #{j+1} channel in #{i+1} executor')
                channel = copy.deepcopy(channel)
                channel.script_dir = '.'
                # check file creation
                try:
                    retcode, outmsg, errmsg = channel.execute_wait(verify_channel_file)
                    if retcode == 0:
                        logger.debug(f'\t\t#{j+1} channel in #{i+1} executor can create file')
                    else:
                        logger.error(f'\t\t#{j+1} channel in #{i+1} executor CANNOT create file with error msg {errmsg}')
                except Exception:
                    raise
                # check mkdir
                try:
                    retcode, outmsg, errmsg = channel.execute_wait(verify_channel_dir)
                    if retcode == 0:
                        logger.debug(f'\t\t#{j+1} channel in #{i+1} executor can mkdir')
                    else:
                        logger.error(f'\t\t#{j+1} channel in #{i+1} executor CANNOT mkdir with error msg {errmsg}')
                except Exception:
                    raise
                # check push_file
                try:
                    dest_file = channel.push_file('./rand.txt', '/tmp')
                    logger.debug(f'\t\t#{j+1} channel in #{i+1} executor can push file')
                except Exception:
                    raise
                # check pull_file
                try:
                    dest_file = channel.pull_file('/tmp/rand.txt', '.')
                    logger.debug(f'\t\t#{j+1} channel in #{i+1} executor can pull file {dest_file}')
                except Exception:
                    raise

    def check_provider(self, disable_launcher):
        logger.debug('\nBegin checking the provider(s) in the config')
        executors = self.config.executors
        logger.debug(f'There are {len(executors)} executors in the config')

        for i, executor in enumerate(executors):
            logger.debug('\tBegin checking the provider in #{} executor, with launcher{wo}'.format(i + 1, wo=' disabled' if disable_launcher else ''))
            provider = copy.deepcopy(executor.provider)
            provider.script_dir = '.'
            if hasattr(provider, 'channel'):
                provider.channel.script_dir = '.'
            else:
                for channel in provider.channels:
                    channel.script_dir = '.'
            if disable_launcher:
                provider.launcher = SimpleLauncher()

            try:
                job_id = provider.submit(verify_provider, 1)
                job_st = provider.status([job_id])[0]
                while not job_st.terminal:  # state == JobState.PENDING or job_st.state == JobState.RUNNING:
                    time.sleep(5)
                    job_st = provider.status([job_id])[0]
                if not job_st.terminal:
                    raise Exception('The provider ({wo} launcher) did not return with terminal status'.format(wo='without' if disable_launcher else 'with'))

                keyval_file = provider.channel.pull_file(os.path.expanduser('~/parsl_tmp/env.txt'), os.getcwd())
                keyval_dict = {line.split("=")[0]: line.split("=")[1] for line in open(keyval_file)}
                process_wp = subprocess.run(['which', 'process_worker_pool.py'], stdout=subprocess.PIPE)

                local_keyval = {'HOSTNAME': socket.gethostname(),
                                'WHICHPYTHON': sys.executable,
                                'PYTHON_VERSION': platform.python_version(),
                                'PARSL_VERSION': parsl.__version__,
                                'PARSL_PATH': parsl.__path__[0],
                                'ZMQ_VERSION': zmq.zmq_version(),
                                'PYZMQ_VERSION': zmq.pyzmq_version(),
                                'PROC_WORKER_POOL': process_wp.stdout.rstrip().decode('utf-8')}

                for key, value in keyval_dict.items():
                    if key == 'HOSTNAME':  # hostname should NOT be the same
                        if value == local_keyval[key]:
                            raise Exception(f'Unexpected {key}={value}')
                    else:
                        if not value == local_keyval[key]:
                            raise Exception(f'Unexpected {key}={value}')

                logger.debug('\tThe provider (with launcher{wo}) returned with terminal status and consistent env variables'
                             .format(wo=' disabled' if disable_launcher else ''))
                shutil.rmtree(os.path.expanduser('~/parsl_tmp'))
                os.remove('./env.txt')

            except Exception:
                raise

    def check_executor(self, func, arg, expected):

        # This method can also check the executors. But the uncommented one
        # is better, bacause it does not involve dfk.
        #
        # dfk = parsl.load(self.config)
        # for label, executor in dfk.executors.items():
        #     fut = executor.submit(square, None, 2)
        #     print(fut.result())

        logger.debug('\nBegin checking the executor(s) in the config')
        executors = self.config.executors
        logger.debug(f'There are {len(executors)} executors in the config')

        avail_spec = {'cores': 2, 'memory': 1000, 'disk': 1000}
        for i, executor in enumerate(executors):
            logger.debug(f'\tBegin checking #{i+1} executor in the config')
            executor.provider.script_dir = '.'
            executor.provider.channel.script_dir = '.'
            block_id = executor.start()
            spec = None
            if type(executor) is WorkQueueExecutor:
                spec = avail_spec
            fut = executor.submit(func, spec, arg)
            try:
                if fut.result() == expected:
                    logger.debug(f'\tWhen starting {block_id}, #{i+1} executor worked properly')
                else:
                    logger.error(f'\tWhen starting {block_id}, #{i+1} executor did NOT work properly')
            except Exception:
                raise


def start_logger(filename, name='parsl', level=logging.DEBUG, format_string=None):
    """Add a file handler and a stream handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandle(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandle(stream_handler)


def cli_run():

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True,
                        help="Config file to be tested")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Set logging debug mode")
    parser.add_argument("-l", "--logdir", default="config_probe_logs",
                        help="Config probe log directory")
    args = parser.parse_args()

    try:
        probe_log_file = os.path.join(args.logdir, 'parsl-probe.log')
        start_logger(probe_log_file,
                     name='probe_log',
                     level=logging.DEBUG if args.debug else logging.INFO)

        config_tester = ConfigTester(args.config_file)
    except Exception:
        raise

    config_tester.check_channel()
    config_tester.check_provider(disable_launcher=True)
    config_tester.check_provider(disable_launcher=False)
    config_tester.check_executor(square, 3, 9)
