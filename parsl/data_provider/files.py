"""Define the File Type.

The primary purpose of the File object is to track the protocol to be used
to transfer the file as well as to give the appropriate filepath depending
on where(client-side, remote-side, intermediary-side) the File.filepath is
being called from
"""

import os
import logging
from urllib.parse import urlparse
from parsl.data_provider.data_manager import DataManager


logger = logging.getLogger(__name__)


class File(str):
    """The Parsl File Class.

    This is planned to be a very simple class that simply
    captures various attributes of a file, and relies on client-side and worker-side
    systems to enable to appropriate transfer of files.
    """

    def __init__(self, url, dman=None, cache=False, caching_dir=".", staging='direct'):
        """Construct a File object from a url string.

        Args:
           - url (string) : url string of the file e.g.
              - 'input.txt'
              - 'file:///scratch/proj101/input.txt'
              - 'globus://go#ep1/~/data/input.txt'
              - 'globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec/home/johndoe/data/input.txt'
           - dman (DataManager) : data manager
        """
        self.url = url
        parsed_url = urlparse(self.url)
        self.scheme = parsed_url.scheme if parsed_url.scheme else 'file'
        self.netloc = parsed_url.netloc
        self.path = parsed_url.path
        self.filename = os.path.basename(self.path)
        self.dman = dman if dman else DataManager.get_data_manager()
        self.data_future = {}
        if self.scheme != 'file':
            self.dman.add_file(self)

        self.cache = cache
        self.caching_dir = caching_dir
        self.staging = staging

    def __str__(self):
        return self.filepath

    def __repr__(self):
        return self.__str__()

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
        if self.scheme == 'globus':
            if hasattr(self, 'local_path'):
                return self.local_path

        if 'exec_site' not in globals() or self.staging == 'direct':
            # Assume local and direct
            return self.path
        else:
            # Return self.path for now
            return self.path

    def stage_in(self, site=None):
        """Transport file from the site of origin to local site."""
        return self.dman.stage_in(self, site)

    def stage_out(self):
        """Transport file from local filesystem to origin site."""
        return self.dman.stage_out(self)

    def set_data_future(self, df, site=None):
        self.data_future[site] = df

    def get_data_future(self, site):
        return self.data_future.get(site)

    def __getstate__(self):
        """ Overriding the default pickling method.

        The File object get's pickled and transmitted to remote sites during app
        execution. This enables pickling while retaining the lockable resources
        to the DFK/Client side.
        """

        state = self.__dict__.copy()

        # We have already made a copy of the future objects, they are now no longer
        # reliable as means to wait for the staging events
        for site in state["data_future"]:
            # This is assumed to be safe, since the data_future represents staging to a specific site
            # and a site will only have one filepath.
            state["data_future"][site] = state["data_future"][site].filepath

        state["dman"] = None

        return state

    def __setstate__(self, state):
        """ Overloading the default pickle method to reconstruct a File from serialized form

        This might require knowledge of whethere a DataManager is already present in the context.
        """
        self.__dict__.update(state)


if __name__ == '__main__':

    x = File('./files.py')
