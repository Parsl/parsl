"""Define the Directory Type.

The primary purpose of the Directory object is to track the protocol to be used
to transfer the directory content as well as to give the appropriate path depending
on where (client-side, remote-side, intermediary-side) the Directory.path is
being called from.
"""
import os

import typeguard
import logging
from typing import Optional, Union
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class Directory:
    """The Parsl Directory Class.

    This represents the global, and sometimes local, URI/path to a directory.

    """

    @typeguard.typechecked
    def __init__(self, url: Union[os.PathLike, str]):
        """Construct a Directory object from a url string.

        Args:
           - url (string or PathLike) : url of the directory e.g.
              - 'falcon://127.0.0.1/data/files/?8080'
        """
        self.url = str(url)
        parsed_url = urlparse(self.url)
        self.scheme = parsed_url.scheme if parsed_url.scheme else 'file'
        self.netloc = parsed_url.netloc
        self.path = parsed_url.path
        self.query = parsed_url.query

    def cleancopy(self) -> "Directory":
        """Returns a copy of the Directory containing only the global immutable state,
           without any mutable site-local local_path information. The returned Directory
           object will be as the original object was when it was constructed.
        """
        logger.debug("Making clean copy of File object {}".format(repr(self)))
        return Directory(self.url)

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        content = f"{type(self).__name__} " \
                  f"at 0x{id(self):x} " \
                  f"url={self.url} " \
                  f"scheme={self.scheme} " \
                  f"netloc={self.netloc} " \
                  f"path={self.path} " \
                  f"query={self.query}"

        return "<{}>".format(content)

    def __fspath__(self) -> str:
        return self.path
