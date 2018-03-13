Doc Docs
========

Tutorial
--------

The tutorial notebooks need to be synced manually for now. When changes are made, the notebook
has to be converted to .rst format, and placed under docs/quick/Tutorial.rst.

The command to perform the conversion is:

    $ jupyter nbconvert --to rst Tutorial.ipynb
    $ sed -i 's/ipython3/python/g' Tutorial.rst


>>> jupyter nbconvert --to rst Tutorial.ipynb
>>> sed -i 's/ipython3/python/g' Tutorial.rst

Remote builds
-------------

Builds are automatically performed by readthedocs.io and published to parsl.readthedocs.io
upon git commits.

Local builds
------------

Dev_Docs
To build the documentation locally, use
::
    $ make html

The developer documentation for the codebase is embedded in the codebase and imported through
docs/devguide/dev_docs.rst. So do not be alarmed to see no text within the .rst file.


