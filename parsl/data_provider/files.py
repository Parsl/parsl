"""Define the File Type.

The primary purpose of the File object is to track the protocol to be used
to transfer the file as well as to give the appropriate filepath depending
on where (client-side, remote-side, intermediary-side) the File.filepath is
being called from.
"""
import logging
from hashlib import md5
import os
import datetime
from typing import Optional, Union
from urllib.parse import urlparse
import uuid

import typeguard

logger = logging.getLogger(__name__)


class File:
    """The Parsl File Class.

    This represents the global, and sometimes local, URI/path to a file.

    Staging-in mechanisms may annotate a file with a local path recording
    the path at the far end of a staging action. It is up to the user of
    the File object to track which local scope that local path actually
    refers to.

    """

    @typeguard.typechecked
    def __init__(self, url: Union[os.PathLike, str], uu_id: Union[uuid.UUID, None] = None,
                 timestamp: Optional[datetime.datetime] = None):
        """Construct a File object from an url string.

        Args:
           - url (string or PathLike) : url of the file e.g.
              - 'input.txt'
              - pathlib.Path('input.txt')
              - 'file:///scratch/proj101/input.txt'
              - 'globus://go#ep1/~/data/input.txt'
              - 'globus://ddb59aef-6d04-11e5-ba46-22000b92c6ec/home/johndoe/data/input.txt'
        """
        self.url = str(url)
        parsed_url = urlparse(self.url)
        self.scheme = parsed_url.scheme if parsed_url.scheme else 'file'
        self.netloc = parsed_url.netloc
        self.path = parsed_url.path
        self.filename = os.path.basename(self.path)
        if self.scheme == 'file' and os.path.isfile(self.path):
            self.size = os.stat(self.path).st_size
            self.md5sum = md5(open(self.path, 'rb').read()).hexdigest()
            self.timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(self.path), tz=datetime.timezone.utc)
        else:
            self.size = None
            self.md5sum = None
            self.timestamp = timestamp
        self.local_path: Optional[str] = None
        if uu_id is not None:
            self.uuid = uu_id
        else:
            self.uuid = uuid.uuid1()

    def cleancopy(self) -> "File":
        """Returns a copy of the file containing only the global immutable state,
           without any mutable site-local local_path information. The returned File
           object will be as the original object was when it was constructed.
        """
        logger.debug("Making clean copy of File object {}".format(repr(self)))
        return File(self.url, self.uuid, self.timestamp)

    def __str__(self) -> str:
        return self.filepath

    def __repr__(self) -> str:
        content = [
            f"{type(self).__name__}",
            f"at 0x{id(self):x}",
            f"url={self.url}",
            f"scheme={self.scheme}",
            f"netloc={self.netloc}",
            f"path={self.path}",
            f"filename={self.filename}",
            f"uuid={self.uuid}",
        ]
        if self.local_path is not None:
            content.append(f"local_path={self.local_path}")

        return f"<{' '.join(content)}>"

    def __fspath__(self) -> str:
        return self.filepath

    @property
    def filepath(self) -> str:
        """Return the resolved filepath on the side where it is called from.

        The appropriate filepath will be returned when called from within
        an app running remotely as well as regular python on the submit side.

        Only file: scheme URLs make sense to have a submit-side path, as other
        URLs are not accessible through POSIX file access.

        Returns:
             - filepath
        """
        if self.local_path is not None:
            return self.local_path

        if self.scheme in ['file']:
            return self.path
        else:
            raise ValueError("No local_path set for {}".format(repr(self)))

    @property
    def timesatmp(self) -> Optional[str]:
        """Stub to make this compatible with DynamicFile objects."""
        return None
