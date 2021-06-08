from parsl.channels.ssh.ssh import SSHChannel as SSH


def connect_and_list(hostname, username):
    conn = SSH(hostname, username=username)
    ec, out, err = conn.execute_wait("echo $HOSTNAME")
    conn.close()
    return out


def test_midway():
    ''' Test ssh channels to midway
    '''
    url = 'midway.rcc.uchicago.edu'
    uname = 'yadunand'
    out = connect_and_list(url, uname)
    print(f"Sitename :{url}  hostname:{out}")


def test_beagle():
    ''' Test ssh channels to beagle
    '''
    url = 'login04.beagle.ci.uchicago.edu'
    uname = 'yadunandb'
    out = connect_and_list(url, uname)
    print(f"Sitename :{url}  hostname:{out}")


def test_osg():
    ''' Test ssh connectivity to osg
    '''
    url = 'login.osgconnect.net'
    uname = 'yadunand'
    out = connect_and_list(url, uname)
    print(f"Sitename :{url}  hostname:{out}")


def test_cori():
    ''' Test ssh connectivity to cori
    '''
    url = 'cori.nersc.gov'
    uname = 'yadunand'
    out = connect_and_list(url, uname)
    print(f"Sitename :{url}  hostname:{out}")


if __name__ == "__main__":

    pass
