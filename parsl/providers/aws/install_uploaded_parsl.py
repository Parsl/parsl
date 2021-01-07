import parsl
from shutil import rmtree
from distutils.dir_util import copy_tree

uploaded_parsl_path = '/tmp/parsl'
pip_parsl_path = parsl.__path__[0]

rmtree(pip_parsl_path)
copy_tree(uploaded_parsl_path, pip_parsl_path)
