import parsl
from shutil import rmtree
from distutils.dir_util import copy_tree
from pathlib import Path

def move(orig, dest):
    try:
        rmtree(dest)
    except Exception:
        pass
    copy_tree(orig, dest)

try:
    orig_parsl_path = '/tmp/parsl'
    dest_parsl_path = parsl.__path__[0]
    move(orig_parsl_path, dest_parsl_path)

    orig_dist_path = '/tmp/parsl-1.0.0.dist-info'
    dest_dist_path = str((Path(dest_parsl_path) / '../parsl-1.0.0.dist-info').resolve())
    move(orig_dist_path, dest_dist_path)
except Exception:
    pass
