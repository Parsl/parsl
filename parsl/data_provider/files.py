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
    def __init__(self, url: str, forced_submit_side: bool = True):
        """Construct a File object from a url string.

        Args:
           - url (string) : url string of the file e.g.
              - 'input.txt'
              - 'file:///scratch/proj101/input.txt'
              - 'globus://go#ep1/~/data/input.txt'
              - 'globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec/home/johndoe/data/input.txt'
         forced_submit_side is an internal consistency check to determine if a File came from a root constructor or from a cleancopy. regular constructions of File will have this set to true
        """
        self.url = url
        parsed_url = urlparse(self.url)
        self.scheme = parsed_url.scheme if parsed_url.scheme else 'file'
        self.netloc = parsed_url.netloc
        self.path = parsed_url.path
        self.filename = os.path.basename(self.path)
        self._local_path = None
        self.forced_submit_side = forced_submit_side

    def cleancopy(self) -> "File":
        """Returns a copy of the file containing only the global immutable state,
           without any mutable site-local local_path information. The returned File
           object will be as the original object was when it was constructed.
        """
        logger.debug("Making clean copy of File object {}".format(repr(self)))
        return File(self.url, forced_submit_side=False)

    def __str__(self):
        return self.filepath

    def __repr__(self):
        content = "{0} at 0x{1:x} url={2} scheme={3} netloc={4} path={5} filename={6}".format(
            self.__class__, id(self), self.url, self.scheme, self.netloc, self.path, self.filename)
        content += " _local_path={0}".format(self._local_path)

        return "<{}>".format(content)

    def __fspath__(self):
        return self.filepath

    @property
    def local_path(self):
        return self._local_path

    @local_path.setter
    def local_path(self, p):
        if self.forced_submit_side:
            raise ValueError("Attempt to set local_path on a forced submit side file: {}".format(repr(self)))

        if self._local_path is None:
            self._local_path = p
        else:
            raise ValueError("Local path is already set for file {}".format(repr(self)))

    @property
    def filepath(self):
        """Return the resolved filepath on the side where it is called from.

        The appropriate filepath will be returned when called from within
        an app running remotely as well as regular python on the submit side.

        Only file: scheme URLs make sense to have a submit-side path, as other
        URLs are not accessible through POSIX file access.

        Args:
            - self
        Returns:
             - filepath (string)
        """
        if self._local_path is not None:
            logger.debug("File {} has filepath from local_path {}".format(repr(self), self._local_path))
            return self.local_path
        elif self.scheme in ['file']:
            logger.debug("File {} filepath from URL path {}".format(repr(self), self.path))
            return self.path
        else:
            raise ValueError("No filepath available for {}".format(repr(self)))


if __name__ == '__main__':

    x = File('./files.py')
