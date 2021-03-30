import parsl
import os
import sys
import platform
import psutil
import shutil
import time
import copy
from importlib.machinery import SourceFileLoader
import subprocess
import zmq
import platform
import glob

from parsl.app.app import python_app, bash_app
from parsl.config import Config
from parsl.utils import wtime_to_minutes
from parsl.providers.provider_base import JobState

from parsl.channels import LocalChannel
from parsl.providers import LocalProvider
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.launchers import SingleNodeLauncher
from parsl.launchers import SimpleLauncher
from parsl.executors import HighThroughputExecutor
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
if [ ! -d ~/parsl_tmp ]; then
    mkdir ~/parsl_tmp
    echo $HOSTNAME                                                         > ~/parsl_tmp/env.txt
    echo `python -c 'import platform;print(platform.python_version())'`   >> ~/parsl_tmp/env.txt
    echo `which python`                                                   >> ~/parsl_tmp/env.txt
    echo `python -c 'import parsl;print(parsl.__version__)'`              >> ~/parsl_tmp/env.txt
    echo `python -c 'import parsl;print(parsl.__path__[0])'`              >> ~/parsl_tmp/env.txt
    echo `python -c 'import zmq;print(zmq.zmq_version())'`                >> ~/parsl_tmp/env.txt
    echo `python -c 'import zmq;print(zmq.pyzmq_version())'`              >> ~/parsl_tmp/env.txt
    echo `which process_worker_pool.py`                                   >> ~/parsl_tmp/env.txt
else
    exit 0
fi
)
'''

def square(x):
    return x*x

class ConfigTester():

    def __init__(self, config):
        try:
            src = SourceFileLoader("config", config).load_module()
            config = src.config
        except:
            raise Exception('Cannot read config file or it does NOT include appropriate config')
        self.config = config

    def check_channel(self):
        print('\nBegin checking the channel(s) in the config')
        executors = self.config.executors
        print(f'There are {len(executors)} executors in the config')

        for i, executor in enumerate(executors):
            channels = None
            if hasattr(executor.provider, 'channel'):
                channels = [ executor.provider.channel ]
            else:
                channels = executor.provider.channels
            print(f'\tThere are {len(channels)} channels in #{i+1} executor')

            for j, channel in enumerate(channels):
                print(f'\t\tBegin checking #{j+1} channel in #{i+1} executor')
                channel = copy.deepcopy(channel)
                channel.script_dir = '.'
                # check file creation
                try:
                    retcode, outmsg, errmsg = channel.execute_wait(verify_channel_file)
                    if retcode == 0:
                        print(f'\t\t#{j+1} channel in #{i+1} executor can create file')
                    else:
                        print(f'\t\t#{j+1} channel in #{i+1} executor CANNOT create file with error msg {errmsg}')
                except Exception as e:
                    raise Exception(f'\t\t#{j+1} channel in #{i+1} executor CANNOT create file with exception {e}')
                # check mkdir
                try:
                    retcode, outmsg, errmsg = channel.execute_wait(verify_channel_dir)
                    if retcode == 0:
                        print(f'\t\t#{j+1} channel in #{i+1} executor can mkdir')
                    else:
                        print(f'\t\t#{j+1} channel in #{i+1} executor CANNOT mkdir with error msg {errmsg}')
                except Exception as e:
                    raise Exception(f'\t\t#{j+1} channel in #{i+1} executor CANNOT mkdir with exception {e}')
                # check push_file
                try:
                    dest_file = channel.push_file('./rand.txt', '/tmp')
                    print(f'\t\t#{j+1} channel in #{i+1} executor can push file')
                except Exception as e:
                    raise Exception(f'\t\t#{j+1} channel in #{i+1} executor CANNOT push file with exception {e}')
                # check pull_file
                try:
                    dest_file = channel.pull_file('/tmp/rand.txt', '.')
                    print(f'\t\t#{j+1} channel in #{i+1} executor can pull file')
                except Exception as e:
                    raise Exception(f'\t\t#{j+1} channel in #{i+1} executor CANNOT pull file with exception {e}')


    def check_provider(self, disable_launcher):
        print('\nBegin checking the provider(s) in the config')
        executors = self.config.executors
        print(f'There are {len(executors)} executors in the config')

        for i, executor in enumerate(executors):
            print('\tBegin checking the provider in #{} executor, with launcher{wo}'.format(i+1, wo=' disabled' if disable_launcher
                                                                                                                else ''))
            provider = copy.deepcopy(executor.provider)
            provider.script_dir = '.'
            if hasattr(provider, 'channel'): 
                provider.channel.script_dir = '.'
            else:
                for channel in provider.channels:
                    channel.script_dir = '.'
            if disable_launcher: provider.launcher = SimpleLauncher()

            try:
                job_id = provider.submit(verify_provider, 1)
                if job_id is None:
                    retcode, stdout, stderr = self._check_provider_with_exectue_wait(provider)
                    raise Exception(f'Met exception with return code {retcode}, and error message {stderr}')

                job_st = provider.status([job_id])[0]
                while not job_st.terminal: # state == JobState.PENDING or job_st.state == JobState.RUNNING:
                    time.sleep(5)
                    job_st = provider.status([job_id])[0]
                if not job_st.terminal:
                    raise Exception('The provider ({wo} launcher) returned, but did not have terminal status'.format(wo = 'without' if disable_launcher else 'with'))

                dest_file = provider.channel.pull_file(os.path.expanduser('~/parsl_tmp/env.txt'), os.getcwd())
                dest_file = open(dest_file)
                lines = dest_file.read().splitlines()
                process_wp = subprocess.run(['which', 'process_worker_pool.py'], stdout=subprocess.PIPE)
                local_env_vars = [ '',
                                   platform.python_version(),
                                   sys.executable,
                                   parsl.__version__,
                                   parsl.__path__[0],
                                   zmq.zmq_version(),
                                   zmq.pyzmq_version(),
                                   process_wp.stdout.rstrip().decode('utf-8')
                                 ]
                if not lines[1] == local_env_vars[1]: raise Exception(f'Unexpected Python version: {lines[1]}')
                if not lines[2] == local_env_vars[2]: raise Exception(f'Unexpected Python path: {lines[2]}')
                if not lines[3] == local_env_vars[3]: raise Exception(f'Unexpected parsl version: {lines[3]}')
                if not lines[4] == local_env_vars[4]: raise Exception(f'Unexpected parsl path: {lines[4]}')
                if not lines[5] == local_env_vars[5]: raise Exception(f'Unexpected zmq version: {lines[5]}')
                if not lines[6] == local_env_vars[6]: raise Exception(f'Unexpected pyzmq version: {lines[6]}')
                if not lines[7] == local_env_vars[7]: raise Exception(f'Unexpected process_worker_pool.py: {lines[7]}')

                print('\tThe provider (with launcher{wo}) returned with terminal status, and the env variables were consistent'.format(wo=' disabled' if disable_launcher else ''))
                shutil.rmtree(os.path.expanduser('~/parsl_tmp'))
                os.remove('./env.txt')

            except Exception as e:
                raise Exception(e)

    def _check_provider_with_exectue_wait(self, provider):
        submitted_files = glob.glob("./parsl.*.submit")
        submitted_files.sort(key=os.path.getmtime, reverse = True)
        last_file = submitted_files[0]
        cmd = None
        if type(provider) is SlurmProvider:
            cmd = f'sbatch {last_file}'
        elif type(provider) is GridEngineProvider:
            if provider.queue is not None:
                cmd = f'qsub -q {provider.queue} -terse {last_file}'
            else:
                cmd = f'qsub -terse {last_file}'
        elif type(provder) is LSFProvider:
            cmd = f'bsub {last_file}'
        elif type(provider) is TorqueProvider:
            submit_options = ''
            if provider.queue is not None:
                submit_options = '{0} -q {1}'.format(submit_options, provider.queue)
            if provider.account is not None:
                submit_options = '{0} -A {1}'.format(submit_options, provider.account)
            cmd = f'qsub {submit_options} {last_file}'
        elif type(provider) is LocalProvider:
            cmd = '/bin/bash -c \'echo - >{0}.ec && {{ {{ bash {0} 1>{0}.out 2>{0}.err ; ' \
                  'echo $? > {0}.ec ; }} >/dev/null 2>&1 & echo "PID:$!" ; }}\''.format(last_file)
        elif type(provider) is CobaltProvider:
            queue_opt = '-q {}'.format(provider.queue) if provider.queue is not None else ''
            account_opt = '-A {}'.format(provider.account) if provider.account is not None else ''
            cmd = 'qsub -n {0} {1} -t {2} {3} {4}'.format(
                  provider.nodes_per_block,
                  queue_opt,
                  wtime_to_minutes(provider.walltime),
                  account_opt,
                  last_file) 
        elif type(provider) is PBSProProvider:
            submit_options = ''
            if provider.queue is not None:
                submit_options = '{0} -q {1}'.format(submit_options, provider.queue)
            if provider.account is not None:
                submit_options = '{0} -A {1}'.format(submit_options, provider.account)
            cmd = f'qsub {submit_options} {last_file}'
        elif type(provider) is CondorProvider:
            cmd = "condor_submit {0}".format(last_file)
        else:
            raise Exception(f'At this time, we do NOT support checking {type(provider)} provider')

        if hasattr(provider, 'execute_wait'):
            return provider.execute_wait(cmd)
        else:
            return provider.channel.execute_wait(cmd, provider.cmd_timeout)

    def check_executor(self, func, arg, expected):

        # This method can also check the executors. But the uncommented one
        # is better, bacause it does not involve dfk.
        #
        # dfk = parsl.load(self.config)
        # for label, executor in dfk.executors.items():
        #     fut = executor.submit(square, None, 2)
        #     print(fut.result())

        print('\nBegin checking the executor(s) in the config')
        executors = self.config.executors
        print(f'There are {len(executors)} executors in the config')

        avail_spec = {'cores': 2, 'memory': 1000, 'disk': 1000}
        for i, executor in enumerate(executors):
            print(f'\tBegin checking #{i+1} executor in the config')
            executor.provider.script_dir = '.'
            executor.provider.channel.script_dir = '.'
            block_id = executor.start()
            spec = None
            if type(executor) is WorkQueueExecutor:
                spec = avail_spec
            fut = executor.submit(func, spec, arg)
            try:
                if fut.result() == expected:
                    print(f'\t#{i+1} executor works properly')
                else:
                    print(f'\t#{i+1} executor does NOT work properly')
            except Exception as e:
                raise Exception(f'\t#{i+1} executor met exception {e}')


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print('Need the path of a config file as a command line argument')
        sys.exit(-1)

    config_tester = ConfigTester(sys.argv[1])

    config_tester.check_channel()
    config_tester.check_provider(disable_launcher=True)
    config_tester.check_provider(disable_launcher=False)
    config_tester.check_executor(square, 3, 9)
