import parsl
from parsl import *
from parsl.execution_provider.provider_factory import ExecProviderFactory

from jinja2 import Environment
from jinja2.loaders import FileSystemLoader

parsl.set_stream_logger()
WORKING_DIR = "working"

# Let's create a pool of threads to execute our functions
# workers = ThreadPoolExecutor(max_workers=4)
# dfk = DataFlowKernel(workers)
config = {"site": "midway_westmere",
          "execution":
          {"executor": "ipp",
           "provider": "slurm",
           "channel": "local",
           "options":
               {"init_parallelism": 2,
                "max_parallelism": 2,
                "min_parallelism": 0,
                "tasks_per_node": 1,
                "node_granularity": 2,
                "partition": "sandyb",
                "walltime": "00:10:00",
                "account": "pi-chard",
                "submit_script_dir": ".scripts"
                }
           }}


import subprocess
import random
print("starting ipcontroller")

rand_port = random.randint(55000, 59000)
p = subprocess.Popen(['ipcontroller', '--reuse',
                      '--port={0}'.format(rand_port), '--ip=*'], stdout=None, stderr=None)

epf = ExecProviderFactory()
resource = epf.make(config)
dfk = DataFlowKernel(resource)


# Helper function to create an input file from a template
def create_template(template_name, output_name, contents):
    fs_loader = FileSystemLoader('templates')
    env = Environment(loader=fs_loader)
    template = env.get_template(template_name)
    packmol_file = open(output_name, 'w')
    packmol_file.write(template.render(contents))
    packmol_file.close()


@App('bash', dfk)
def mpi_hello(ranks, inputs=[], outputs=[], stdout=None, stderr=None, mock=False):
    pass


@App('bash', dfk)
def mpi_test(ranks, inputs=[], outputs=[], stdout=None, stderr=None, mock=False):
    return """module load amber/16+cuda-8.0
    mpirun -n 6 mpi_hello
    mpirun -np 6 pmemd.MPI -O -i config_files/min.in -o min.out -c prot.rst7 -p prot.parm7 -r min.rst7 -ref prot.rst7
    """


# x = mpi_hello(4, stdout="hello.out", stderr="hello.err")
# print("Launched the mpi_hello app")
# x.result()

x = mpi_test(4, stdout="hello.out", stderr="hello.err")
print("Launched the mpi_hello app")
x.result()

p.terminate()
