Swan notes
==========

Environment
===========
Swan has a module cray-python/3.6.1.1 which can be used readily to setup and virtual
env::

    module load cray-python/3.6.1.1
    python3 -m venv parsl_env
    source parsl_env/bin/activate
    pip install parsl
