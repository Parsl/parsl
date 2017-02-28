Doc Docs
========


Tutorial
--------

The tutorial notebooks need to be synced manually for now. When changes are made, the notebook
has to be converted to .rst format, and placed under docs/quick/Tutorial.rst.

The command to make the convertion is:

>>> jupyter nbconvert --to rst Tutorial.ipynb
>>> sed -i 's/ipython3/python/g' Tutorial.rst

Builds
------

As of now, builds are automatically done by readthedocs.io and published to parsl.readthedocs.io
upon git commits.


