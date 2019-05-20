# data manager from benc's perspective


## requirements

this is a bit of a random grab bag from my head - needs to align with what
is in the shared google doc.

support per-task workdirs (for bash apps - for python apps there is no
per-task pwd to be able to change)

support mode where no filesystem shared between different workers in
the same executor

support complex movements - for example globus transfer from remote location
into a worker which has no shared fs (for example, by going via submit-side
or other location)

support staging from the submit side local file system


