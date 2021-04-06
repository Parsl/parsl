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
if ( [ ! -d ~/parsl_tmp ] && mkdir ~/parsl_tmp ) 2> /dev/null
then
    echo "HOSTNAME=$HOSTNAME"                                                 > ~/parsl_tmp/env.txt
    echo "WHICHPYTHON=$(which python)"                                       >> ~/parsl_tmp/env.txt
    echo -n "PYTHON_VERSION="                                                >> ~/parsl_tmp/env.txt
    python --version                                                         >> ~/parsl_tmp/env.txt
    echo "PARSL_VERSION="`python -c 'import parsl;print(parsl.__version__)'` >> ~/parsl_tmp/env.txt
    echo "PARSL_PATH="`python -c 'import parsl;print(parsl.__path__[0])'`    >> ~/parsl_tmp/env.txt
    echo "ZMQ_VERSION="`python -c 'import zmq;print(zmq.zmq_version())'`     >> ~/parsl_tmp/env.txt
    echo "PYZMQ_VERSION="`python -c 'import zmq;print(zmq.pyzmq_version())'` >> ~/parsl_tmp/env.txt
    echo "PROC_WORKER_POOL="`which process_worker_pool.py`                   >> ~/parsl_tmp/env.txt
else
    exit 0
fi
)
'''


def square(x):
    return x * x


class ConfigTester():

    def __init__(self, config):
        self.config = config
        if os.path.isdir('~/parsl_tmp'):
            shutil.rmtree('~/parsl_tmp')

    def check_channel(self):
        logger.debug('Begin checking the channel(s) in the config')
        executors = self.config.executors
        logger.debug(f'There are {len(executors)} executors in the config')

        for i, executor in enumerate(executors):
            channels = None
            if hasattr(executor.provider, 'channel'):
                channels = [executor.provider.channel]
            else:
                channels = executor.provider.channels
            logger.debug(f'\tThere are {len(channels)} channels in the {executor.__class__.__name__}')

            for j, channel in enumerate(channels):
                logger.debug(f'\t\tBegin checking the {channel.__class__.__name__} in the {executor.__class__.__name__}')
                channel = copy.deepcopy(channel)
                channel.script_dir = '.'
                # check file creation
                try:
                    retcode, outmsg, errmsg = channel.execute_wait(verify_channel_file)
                    if retcode == 0:
                        logger.debug(f'\t\t{channel.__class__.__name__} creating file: SUCCEEDED')
                    else:
                        logger.exception(f'\t\t{channel.__class__.__name__} creating file: FAILED, error message {errmsg}')
                except Exception:
                    logger.exception(f'\t\t{channel.__class__.__name__} creating file: FAILED')
                    raise
                # check mkdir
                try:
                    retcode, outmsg, errmsg = channel.execute_wait(verify_channel_dir)
                    if retcode == 0:
                        logger.debug(f'\t\t{channel.__class__.__name__} creating directory: SUCCEEDED')
                    else:
                        logger.exception(f'\t\t{channel.__class__.__name__} creating directory: FAILED, error message {errmsg}')
                except Exception:
                    logger.exception(f'\t\t{channel.__class__.__name__} creating directory: FAILED')
                    raise
                # check push_file
                try:
                    dest_file = channel.push_file('./rand.txt', '/tmp')
                    logger.debug(f'\t\t{channel.__class__.__name__} pushing file: SUCCEEDED')
                except Exception:
                    logger.exception(f'\t\t{channel.__class__.__name__} pushing file: FAILED')
                    raise
                # check pull_file
                try:
                    dest_file = channel.pull_file('/tmp/rand.txt', '.')
                    logger.debug(f'\t\t{channel.__class__.__name__} pulling file: SUCCEEDED')
                except Exception:
                    logger.exception(f'\t\t{channel.__class__.__name__} pulling {dest_file}: FAILED')
                    raise

    def check_provider(self, disable_launcher):
        logger.debug('Begin checking the provider(s) in the config')
        executors = self.config.executors
        logger.debug(f'There are {len(executors)} executors in the config')

        for i, executor in enumerate(executors):
            provider = copy.deepcopy(executor.provider)
            provider.script_dir = '.'
            logger.debug('\tBegin checking the {prd} in {ex}, with launcher{wo}'.format(
                         prd=provider.__class__.__name__, ex=executor.__class__.__name__, wo=' disabled' if disable_launcher else ''))
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
                    msg = '{prd} ({wo} launcher) submit: FAILED, without terminal status'.format(
                           prd=provider.__class__.__name__, wo='without' if disable_launcher else 'with')
                    logger.exception(msg)
                    raise Exception(msg)

                keyval_file = provider.channel.pull_file(os.path.expanduser('~/parsl_tmp/env.txt'), os.getcwd())
                keyval_dict = {line.split("=")[0]: line.split("=")[1].strip() for line in open(keyval_file)}
                process_wp = subprocess.run(['which', 'process_worker_pool.py'], stdout=subprocess.PIPE)

                local_keyval = {'HOSTNAME': socket.gethostname(),
                                'WHICHPYTHON': sys.executable,
                                'PYTHON_VERSION': 'Python ' + platform.python_version(),
                                'PARSL_VERSION': parsl.__version__,
                                'PARSL_PATH': parsl.__path__[0],
                                'ZMQ_VERSION': zmq.zmq_version(),
                                'PYZMQ_VERSION': zmq.pyzmq_version(),
                                'PROC_WORKER_POOL': process_wp.stdout.rstrip().decode('utf-8')}

                for key, value in keyval_dict.items():
                    if key == 'HOSTNAME':  # hostname should NOT be the same
                        if value == local_keyval[key]:
                            msg = f'{provider.__class__.__name__} submit: succeeded, BUT returned UNEXPECTED {key}={value}'
                            logger.exception(msg)
                            raise Exception(msg)
                    else:
                        if not value == local_keyval[key]:
                            msg = f'{provider.__class__.__name__} submit: succeeded, BUT returned UNEXPECTED {key}={value}, \
                                    expecting {local_keyval[key]}'
                            logger.exception(msg)
                            raise Exception(msg)

                logger.debug('\t{prd} (with launcher{wo}) submit: SUCCEEDED'.format(
                             prd=provider.__class__.__name__, wo=' disabled' if disable_launcher else ''))
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

        logger.debug('Begin checking the executor(s) in the config')
        executors = self.config.executors
        logger.debug(f'There are {len(executors)} executors in the config')

        for i, executor in enumerate(executors):
            logger.debug(f'\tBegin checking {executor.__class__.__name__} in the config')
            executor.provider.script_dir = '.'
            executor.provider.channel.script_dir = '.'
            block_id = executor.start()
            fut = executor.submit(func, None, arg)
            try:
                if fut.result() == expected:
                    logger.debug(f'\t{executor.__class__.__name__} submit: SUCCEEDED')
                else:
                    logger.exception(f'\t{executor.__class__.__name__} submit: FAILED in block {block_id}')
                executor.shutdown()
            except Exception:
                raise

    def check_all(self):
        self.check_channel()
        time.sleep(10)
        self.check_provider(disable_launcher=True)
        time.sleep(15)
        self.check_provider(disable_launcher=False)
        time.sleep(15)
        self.check_executor(square, 3, 9)


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
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def cli_run():

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True,
                        help="Config file to be tested")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Set logging debug mode")
    args = parser.parse_args()

    try:
        probe_log_file = os.path.join('.', 'parsl-probe.log')
        start_logger(probe_log_file,
                     name="parsl/probe/probe.py",
                     level=logging.DEBUG if args.debug else logging.INFO)

        src = SourceFileLoader("config", args.file).load_module()
        config = src.config
        config_tester = ConfigTester(config)
    except Exception:
        raise

    config_tester.check_all()


def check_config(config, debug=True):
    try:
        probe_log_file = os.path.join('.', 'parsl-probe.log')
        start_logger(probe_log_file,
                     name="parsl/probe/probe.py",
                     level=logging.DEBUG if debug else logging.INFO)

        config_tester = ConfigTester(config)
    except Exception:
        raise

    config_tester.check_all()
