

class File(object):
    ''' The Parsl File Class. This is planned to be a very simple class that simply
    captures various attributes of a file, and relies on client-side and worker-side
    systems to enable to appropriate transfer of files.
    '''

    def __init__ (self, url, cache=False, caching_dir=".", staging='direct'):
        ''' Construct a File object from a url string

        Args:
             - url (string) : url string of the file eg.
        '''

        self.url = url
        *protocol, path = self.url.split('://', 1)

        self.protocol = protocol[0] if protocol else 'file'
        self.path = path
        self.cache = cache
        self.staging = staging

    def __str__ (self):
        return self.url

    def __repr__ (self):
        return self.__str__()

    @property
    def filepath(self):
        if 'exec_site' not in globals() or self.staging == 'direct':
            # Assume local and direct
            return self.path
        else:
            # Return self.path for now
            return self.path

