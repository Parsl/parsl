"""Define the File Type.

The primary purpose of the File object is to track the protocol to be used
to transfer the file as well as to give the appropriate filepath depending
on where (client-side, remote-side, intermediary-side) the File.filepath is
being called from.
"""

import os
import typeguard
import logging
from typing import Dict
from urllib.parse import urlparse

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from parsl.app.futures import DataFuture


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
        self.data_future = {}  # type: Dict[str, DataFuture]

    def __str__(self):
        return self.filepath

    def __repr__(self):
        return self.__str__()

    def __fspath__(self):
        return self.filepath

    def is_remote(self):
        if self.scheme in ['ftp', 'http', 'https', 'globus']:
            return True
        elif self.scheme in ['file']:  # TODO: is this enough?
            return False
        else:
            raise Exception('Cannot determine if unknown file scheme {} is remote'.format(self.scheme))

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

    def __getstate__(self):
        """Override the default pickling method.

        The File object gets pickled and transmitted to remote executors during app
        execution. This enables pickling while retaining the lockable resources
        to the DFK/Client side.
        """

        state = self.__dict__.copy()

        # We have already made a copy of the future objects, they are now no longer
        # reliable as means to wait for the staging events
        for executor in state["data_future"]:
            # This is assumed to be safe, since the data_future represents staging to a specific executor
            # and an executor will only have one filepath.
            state["data_future"][executor] = state["data_future"][executor].filepath

        return state

    def __setstate__(self, state):
        """ Overloading the default pickle method to reconstruct a File from serialized form

        This might require knowledge of whether a DataManager is already present in the context.
        """
        self.__dict__.update(state)


if __name__ == '__main__':

    x = File('./files.py')
