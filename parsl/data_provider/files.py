"""Define the File Type.

The primary purpose of the File object is to track the protocol to be used
to transfer the file as well as to give the appropriate filepath depending
on where (client-side, remote-side, intermediary-side) the File.filepath is
being called from.
"""

import os
import typeguard
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class File(object):
    """The Parsl File Class.

    This represents the global, and sometimes local, URI/path to a file.

    Staging-in mechanisms may annotate a file with a local path recording
    the path at the far end of a staging action. It is up to the user of
    the File object to track which local scope that local path actually
    refers to.

    """

    @typeguard.typechecked
    def __init__(self, url: str):
        """Construct a File object from a url string.

        Args:
           - url (string) : url string of the file e.g.
              - 'input.txt'
              - 'file:///scratch/proj101/input.txt'
              - 'globus://go#ep1/~/data/input.txt'
              - 'globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec/home/johndoe/data/input.txt'
        """
        self.url = url
        parsed_url = urlparse(self.url)
        self.scheme = parsed_url.scheme if parsed_url.scheme else 'file'
        self.netloc = parsed_url.netloc
        self.path = parsed_url.path
        self.filename = os.path.basename(self.path)

    def __str__(self):
        return self.filepath

    def __repr__(self):
        if hasattr(self, 'local_path'):
            return "<{0} at 0x{1:x} url={2} local_path={3}>".format(self.__class__, id(self), self.url, self.local_path)
        else:
            return "<{0} at 0x{1:x} url={2}>".format(self.__class__, id(self), self.url)

    def __fspath__(self):
        return self.filepath

    @property
    def filepath(self):
        """Return the resolved filepath on the side where it is called from.

        The appropriate filepath will be returned when called from within
        an app running remotely as well as regular python on the client side.

        Args:
            - self
        Returns:
             - filepath (string)
        """
        if hasattr(self, 'local_path'):
            return self.local_path

        if self.scheme in ['ftp', 'http', 'https', 'globus']:
            return self.filename
        elif self.scheme in ['file']:
            return self.path
        else:
            raise Exception('Cannot return filepath for unknown scheme {}'.format(self.scheme))


if __name__ == '__main__':

    x = File('./files.py')
