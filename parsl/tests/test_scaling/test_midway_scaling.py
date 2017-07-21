#!/usr/bin/env python3

import parsl
from parsl.execution_provider.midway.slurm import Midway

if __name__ == "__main__" :

    conf = { "site" : "pool1",
             "queue" : "bigmem",
             "maxnodes" : 4,
             "walltime" : '00:04:00',
             "controller" : "10.50.181.1:50001" }

    pool1 = Midway(conf)
    pool1.scale_out(1)
    pool1.scale_out(1)
    print("Pool resources : ", pool1.resources)
    pool1.status()
    pool1.scale_in(1)
    pool1.scale_in(1)


