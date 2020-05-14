#!/usr/bin/env python3


"""Example for vasp.

[Notes from Dan]
1 - a python code that runs and creates a tar file - it writes the tar file directly,
    rather than creating the contents and then tarring them, though someone likely could
    change this to create the contents directly with some work.
2 - if we open the tar file, we get a bunch of stuff that includes - some directories
    called relax.xy, some directories called neb.xy, and a Makefile.
3 - We need to run VASP in each relax.xy directly - in our test case, there are 4 such
    directories.  Each VASP run will take 10-30 hours on O(100) cores.
4 - once these have finished, we run make, which uses the results in the relax.xy
    directories to build the inputs in the deb.xy directories - perhaps we could
    figure out what Make does and do it in python instead, but likely, we could
    just call Make from python...
5 - We can then run VASP in the deb.xy directories - in our test case, there are 17
    such directories, with similar VASP runtimes as before.
6 - Once these are done, we need to run some more python code that we don't actually
    have yet, but that a student here supposedly does have written and tested.

We will be working on Stampede 2. we haven't put our code in a repo (though we should - Qingyi...)
   and everything we used can be installed via pip.

"""

from parsl import bash_app, python_app, DataFlowKernel, ThreadPoolExecutor
import os
import shutil
import random

workers = ThreadPoolExecutor(max_workers=8)
dfk = DataFlowKernel(workers)


def create_dirs(cwd):

    for dir in ['relax.01', 'relax.02', 'relax.03']:
        rel_dir = '{0}/{1}'.format(cwd, dir)
        if os.path.exists(rel_dir):
            shutil.rmtree(rel_dir)
        os.makedirs(rel_dir)
        for i in range(0, random.randint(1, 5)):
            rdir = '{0}/{1}'.format(rel_dir, i)
            os.makedirs(rdir)
            with open('{0}/results'.format(rdir), 'w') as f:
                f.write("{0} {1} - test data\n".format(i, dir))

    for dir in ['neb01', 'neb02', 'neb03', 'neb04']:
        rel_dir = '{0}/{1}'.format(cwd, dir)
        if os.path.exists(rel_dir):
            shutil.rmtree(rel_dir)
        os.makedirs(rel_dir)
        with open('{0}/{1}.txt'.format(rel_dir, dir), 'w') as f:
            f.write("{0} test data\n".format(rel_dir))


@python_app(data_flow_kernel=dfk)
def ls(pwd, outputs=[]):
    import os
    items = os.listdir(pwd)
    with open(outputs[0], 'w') as f:
        f.write(' '.join(items))
        f.write('\n')
    # Returning list of items in current dir as python object
    return items


@bash_app(data_flow_kernel=dfk)
def catter(dir, outputs=[], stdout=None, stderr=None):
    pass


if __name__ == "__main__":

    pwd = os.getcwd()
    create_dirs(pwd)

    # Listing the cwd
    ls_fu, [res] = ls(pwd, outputs=['results'])

    dir_fus = {}
    for dir in ls_fu.result():
        if dir.startswith('relax') and os.path.isdir(dir):
            print("Launching {0}".format(dir))
            dir_fus[dir] = catter(dir, outputs=['{0}.allresults'.format(dir)],
                                  stderr='{0}.stderr'.format(dir))

    for dir in dir_fus:
        try:
            print(dir_fus[dir][0].result())
        except Exception as e:
            print("Caught exception{0}  on {1}".format(e, dir))
