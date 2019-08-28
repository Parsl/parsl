import os

from parsl.channels.ssh.ssh import SSHChannel as SSH


def connect_and_list(hostname, username):
    out = ''
    conn = SSH(hostname, username=username)
    conn.push_file(os.path.abspath('remote_run.sh'), '/home/davidk/')
    # ec, out, err = conn.execute_wait("ls /tmp/remote_run.sh; bash /tmp/remote_run.sh")
    conn.close()
    return out


script = '''#!/bin/bash
echo "Hostname: $HOSTNAME"
echo "Cpu info -----"
cat /proc/cpuinfo
echo "Done----------"
'''


def test_connect_1():
    with open('remote_run.sh', 'w') as f:
        f.write(script)

    sites = {
        'midway': {
            'url': 'midway.rcc.uchicago.edu',
            'uname': 'yadunand'
        },
        'swift': {
            'url': 'swift.rcc.uchicago.edu',
            'uname': 'yadunand'
        },
        'cori': {
            'url': 'cori.nersc.gov',
            'uname': 'yadunand'
        }
    }

    for site in sites.values():
        out = connect_and_list(site['url'], site['uname'])
        print("Sitename :{0}  hostname:{1}".format(site['url'], out))


if __name__ == "__main__":

    test_connect_1()
