import os

import pytest

import parsl
from parsl.app.app import App
from parsl.tests.configs.midway_ipp_multicore import config


local_config = config


@App('bash')
def mpi_hello(ranks, inputs=[], outputs=[], stdout=None, stderr=None, mock=False):
    pass


@App('bash')
def mpi_test(ranks, inputs=[], outputs=[], stdout=None, stderr=None, mock=False):
    return """module load amber/16+cuda-8.0
    mpirun -n 6 mpi_hello
    mpirun -np 6 pmemd.MPI -O -i config_files/min.in -o min.out -c prot.rst7 -p prot.parm7 -r min.rst7 -ref prot.rst7
    """


whitelist = os.path.join(os.path.dirname(parsl.__file__), 'tests', 'configs', '*MPI.py')


# @pytest.mark.whitelist(whitelist)
@pytest.mark.skip("Broke somewhere between PR #525 and PR #652")
def test_mpi():
    x = mpi_test(4, stdout="hello.out", stderr="hello.err")
    print("Launched the mpi_hello app")
    x.result()
