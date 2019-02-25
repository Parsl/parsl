"""Define the File Type.

The primary purpose of the File object is to track the protocol to be used
to transfer the file as well as to give the appropriate filepath depending
on where (client-side, remote-side, intermediary-side) the File.filepath is
being called from.
"""

import os
import logging
from urllib.parse import urlparse
from parsl.data_provider.data_manager import DataManager


logger = logging.getLogger(__name__)


class File(str):
    """The Parsl File Class.

    This class captures various attributes of a file, and relies on client-side and
    worker-side systems to enable to appropriate transfer of files.

    Note that an error will be raised if one tries to create a File without an
    associated DataManager. That DataManager may be specified explicitly
    as a parameter to the File constructor, or may be implicitly specified via
    a previously loaded Parsl config.

    A File which is not associated with a DataManager is ill-defined.
    *** BENC ^ I'd like there to be no DataManager association...


    TODO:
    This was a str subclass, but I don't see that it needs to be - I should
    understand what, if anything, was coming from that inheritence? Other than
    matching `isinstance str` tests which I think were then immediatly countermanded
    by `isinstance File`

    It does seem to break some tests changing the superclass, though...

    Where it is needed is when we pass a File into open(), open wants a string
    filename (or something like it) which we aren't providing. So open() fails as
    having been passed an invalid filename.

    That's slightly frustrating because this will change the str() representation
    as we move between platforms, so it won't necessarily commute with other string
    operations.

    Maybe have a thing about the ergonomics of what datafuture returns and what
    a File has as methods.

    Do I want to require str() on all future results, to fix this? it seems awkward
    even if it is more type correct.


    This class specifically supports PEP-0519 which provides an __fspath__
    method to support this kind of file-system-path-like object without
    needing a str subclass. But that is only supported with Python 3.6
    or later.

    That's maybe an argument for tolerating string now - keeping semantics
    such that it should behave as a `str` subclass as if it was not a str
    subclass but has a suitable __fspath__ ? ... and document that properly
    as a desired long term goal if <3.6 support is dropped.


    """

    def __init__(self, url, dman=None):
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
        if self.scheme in ['ftp', 'http', 'https', 'globus']:
            # The path returned here has to match exactly with where the
            # TODO: ^ .... ?
            if hasattr(self, 'local_path'):
                return self.local_path
            else:
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

        state["dman"] = None

        return state

    def __setstate__(self, state):
        """ Overloading the default pickle method to reconstruct a File from serialized form

        This might require knowledge of whether a DataManager is already present in the context.
        """
        self.__dict__.update(state)


if __name__ == '__main__':

    x = File('./files.py')
