#!/usr/bin/env python3

import sys

import radical.utils as ru
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # The purpose of this master is to (a) spawn a set or workers
    # within the same allocation, (b) to distribute work items to
    # those workers, and (c) to collect the responses again.
    cfg_fname = str(sys.argv[1])
    cfg = ru.Config(cfg=ru.read_json(cfg_fname))
    cfg.rank = int(sys.argv[2])

    worker_descr = cfg.worker_descr
    n_workers = cfg.n_workers
    gpus_per_node = cfg.gpus_per_node
    cores_per_node = cfg.cores_per_node
    nodes_per_worker = cfg.nodes_per_worker

    # create a master class instance - this will establish communication
    # to the pilot agent
    master = rp.raptor.Master(cfg)

    # insert `n` worker into the agent.  The agent will schedule (place)
    # those workers and execute them.
    worker_descr['ranks'] = nodes_per_worker * cores_per_node
    worker_descr['gpus_per_rank'] = nodes_per_worker * gpus_per_node
    worker_ids = master.submit_workers(
                 [rp.TaskDescription(worker_descr) for _ in range(n_workers)])

    # wait for all workers
    master.wait_workers()
    master.start()
    master.join()

# ------------------------------------------------------------------------------
