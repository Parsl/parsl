''' Define the File Type.

The primary purpose of the File object is to track the protocol to be used
to transfer the file as well as to give the appropriate filepath depending
on where(client-side, remote-side, intermediary-side) the File.filepath is
being called from
'''

class File(object):
    ''' The Parsl File Class. This is planned to be a very simple class that simply
    captures various attributes of a file, and relies on client-side and worker-side
    systems to enable to appropriate transfer of files.
    '''

    def __init__(self, url, cache=False, caching_dir=".", staging='direct'):
        ''' Construct a File object from a url string

        Args:
             - url (string) : url string of the file eg.
        '''

        self.url = url
        *protocol, path = self.url.split('://', 1)

        self.protocol = protocol[0] if protocol else 'file'
        self.path = path
        self.cache = cache
        self.caching_dir = caching_dir
        self.staging = staging

    def __str__(self):
        return self.url

    def __repr__(self):
        return self.__str__()

    @property
    def filepath(self):
        ''' Returns the resolved filepath on the side where it is called from.
        File.filepath returns the appropriate filepath when called from within
        an app running remotely as well as regular python on the client side.

        Args:
            - self
        Returns:
             - filepath (string)

        '''
        if 'exec_site' not in globals() or self.staging == 'direct':
            # Assume local and direct
            return self.path
        else:
            # Return self.path for now
            return self.path

    def stage_in(self):
        ''' The stage_in call transports the file from the side of origin
        to the local side
        '''
        pass

    def stage_out(self):
        ''' The stage_out call transports the file from local filesystem
        to the origin side
        '''
        pass


if __name__ == '__main__' :

    x = File('./files.py')

