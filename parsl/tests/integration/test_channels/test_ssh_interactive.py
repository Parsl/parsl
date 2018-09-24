import parsl
from parsl.channels.ssh_il.ssh_il import SSHInteractiveLoginChannel as SSH


def connect_and_list(hostname, username):
    conn = SSH(hostname, username=username)
    ec, out, err = conn.execute_wait("echo $HOSTNAME")
    conn.close()
    return out


def test_cooley():
    ''' Test ssh channels to midway
    '''
    url = 'cooley.alcf.anl.gov'
    uname = 'yadunand'
    out = connect_and_list(url, uname)
    print("Sitename :{0}  hostname:{1}".format(url, out))
    return


if __name__ == "__main__":
    parsl.set_stream_logger()
    test_cooley()
