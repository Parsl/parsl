import os
import pathlib
import pytest
import random
import shutil
import socket
import subprocess
import tempfile
import threading
import time

from parsl.channels import LocalChannel, SSHChannel
from parsl.launchers import SingleNodeLauncher
from parsl.providers import LocalProvider
from parsl.providers.provider_base import JobState


def _run_tests(p: LocalProvider):
    status = _run(p, '/bin/true')
    assert status.message is None
    assert status.state == JobState.COMPLETED
    assert status.exit_code == 0
    assert status.stdout == ''
    assert status.stderr == ''

    status = _run(p, '/bin/true', np=2)
    assert status.message is None
    assert status.state == JobState.COMPLETED
    assert status.exit_code == 0
    assert status.stdout == ''
    assert status.stderr == ''

    status = _run(p, '/bin/false')
    assert status.state == JobState.FAILED
    assert status.exit_code != 0
    assert status.stdout == ''
    assert status.stderr == ''

    status = _run(p, '/bin/false', np=2)
    assert status.state == JobState.FAILED
    assert status.exit_code != 0
    assert status.stdout == ''
    assert status.stderr == ''

    status = _run(p, '/bin/echo -n magic')
    assert status.state == JobState.COMPLETED
    assert status.exit_code == 0
    assert status.stdout == 'magic'
    assert status.stderr == ''

    status = _run(p, '/bin/echo -n magic 1>&2')
    assert status.state == JobState.COMPLETED
    assert status.exit_code == 0
    assert status.stdout == ''
    assert status.stderr == 'magic'


@pytest.mark.local
def test_local_channel():
    with tempfile.TemporaryDirectory() as script_dir:
        script_dir = tempfile.mkdtemp()
        p = LocalProvider(channel=LocalChannel(), launcher=SingleNodeLauncher(debug=False))
        p.script_dir = script_dir
        _run_tests(p)


# It would probably be better, when more formalized site testing comes into existence, to
# use a site-testing provided server/configuration instead of the current scheme
@pytest.mark.local
def test_ssh_channel():
    with tempfile.TemporaryDirectory() as config_dir:
        sshd_thread, priv_key, server_port = _start_sshd(config_dir)
        try:
            with tempfile.TemporaryDirectory() as remote_script_dir:
                # The SSH library fails to add the new host key to the file if the file does not
                # already exist, so create it here.
                pathlib.Path(f'{config_dir}/known.hosts').touch(mode=0o600)
                script_dir = tempfile.mkdtemp()
                channel_ = SSHChannel('127.0.0.1', port=server_port,
                                      script_dir=remote_script_dir,
                                      host_keys_filename=(f'{config_dir}'
                                                          f'/known.hosts'),
                                      key_filename=priv_key)
                p = LocalProvider(channel=channel_,
                                  launcher=SingleNodeLauncher(debug=False))
                p.script_dir = script_dir
                _run_tests(p)
        finally:
            _stop_sshd(sshd_thread)


def _stop_sshd(sshd_thread):
    sshd_thread.stop()


class SSHDThread(threading.Thread):
    def __init__(self, config_file):
        threading.Thread.__init__(self, daemon=True)
        self.config_file = config_file
        self.stop_flag = False
        self.error = None

    def run(self):
        try:
            # sshd needs to be run with an absolute path, hence the call to which()
            p = subprocess.Popen([shutil.which('sshd'), '-D', '-f', self.config_file],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            while True:
                ec = p.poll()
                if self.stop_flag:
                    p.terminate()
                    break
                elif ec is None:
                    time.sleep(0.1)
                elif ec == 0:
                    self.error = Exception(f'sshd exited prematurely: '
                                           f'{p.stdout.read()}'
                                           f'{p.stderr.read()}')
                    break
                else:
                    self.error = Exception(f'sshd failed: '
                                           f'{p.stdout.read()}'
                                           f'{p.stderr.read()}')

                    break
        except Exception as ex:
            self.error = ex

    def stop(self):
        self.stop_flag = True


def _start_sshd(config_dir: str):
    server_config, priv_key, port = _init_sshd(config_dir)
    sshd_thread = SSHDThread(server_config)
    sshd_thread.start()
    time.sleep(1.0)
    if not sshd_thread.is_alive():
        raise Exception(f'Failed to start sshd: {sshd_thread.error}')
    return sshd_thread, priv_key, port


def _init_sshd(config_dir):
    hostkey = f'{config_dir}/hostkey'
    connkey = f'{config_dir}/connkey'
    os.system(f'ssh-keygen -b 2048 -t rsa -q -N "" -f {hostkey}')
    os.system(f'ssh-keygen -b 2048 -t rsa -q -N "" -f {connkey}')
    port = _find_free_port(22222)
    server_config_str = f"""
    Port {port}
    ListenAddress 127.0.0.1
    HostKey {hostkey}
    AuthorizedKeysFile {f'{connkey}.pub'}
    AuthenticationMethods publickey
    StrictModes no
    Subsystem sftp {_get_system_sftp_path()}
    """
    server_config = f'{config_dir}/sshd_config'
    with open(server_config, 'w') as f:
        f.write(server_config_str)
    return server_config, connkey, port


def _get_system_sftp_path():
    try:
        with open('/etc/ssh/sshd_config') as f:
            line = f.readline()
            while line:
                tokens = line.split()
                if tokens[0] == 'Subsystem' and tokens[1] == 'sftp':
                    return tokens[2]
                line = f.readline()
    except Exception:
        pass
    return '/usr/lib/openssh/sftp-server'


def _find_free_port(start: int):
    port = start
    while port < 65535:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(('127.0.0.1', port))
            s.close()
            return port
        except Exception:
            pass
        port += random.randint(1, 20)
    raise Exception('Could not find free port')


def _run(p: LocalProvider, command: str, np: int = 1):
    id = p.submit(command, 1, np)
    return _wait(p, id)


def _wait(p: LocalProvider, id: object):
    status = p.status([id])[0]
    while not status.terminal:
        time.sleep(0.1)
        status = p.status([id])[0]
    return status
