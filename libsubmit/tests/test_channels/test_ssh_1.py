from libsubmit.channels.ssh.ssh import SshChannel as Ssh

def connect_and_list(hostname, username):
    conn = Ssh(hostname, username=username)
    ec, out, err = conn.execute_wait("echo $HOSTNAME")
    conn.close()
    return out


def test_connect_1 ():

    sites = {'midway' : {'url': 'midway.rcc.uchicago.edu',
                         'uname' : 'yadunand'},
             'swift'  : {'url': 'swift.rcc.uchicago.edu',
                         'uname' : 'yadunand'},
             'cori'   : {'url': 'cori.nersc.gov',
                         'uname' : 'yadunand'}
             }

    for site in sites.values():
        out = connect_and_list(site['url'], site['uname'])
        print("Sitename :{0}  hostname:{1}".format(site['url'],
                                                   out))
if __name__ == "__main__":

    test_connect_1()
