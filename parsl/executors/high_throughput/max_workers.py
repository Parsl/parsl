import logging
import math
from typing import Optional, Sequence

logger = logging.getLogger(__name__)


def compute_max_workers(*,
                        mem_per_node: Optional[float],
                        mem_per_worker: Optional[float],
                        cores_per_node: Optional[float],
                        cores_per_worker: Optional[float],
                        configured_max_workers_per_node: Optional[int],
                        accelerators: Sequence) -> Optional[int]:
    """Calculate a maximum worker count.

    Each parameter is optional: either as None or as an empty sequence.

    If there is no maximum, because there are insufficient constraints, then
    return None.

    mem_per_node: optional float, GB
      total memory on a worker node

    mem_per_worker: optional float, GB
      memory to be allocated to each worker

    cores_per_node: optional float
      number of cores on a worker node. To disambiguate the overloaded
      use of cpu, core and cpu thread in different places:  Cores in this context
      generally should be read in the sense of the number returned by SpawnContext.cpu_count()
      and os.cpu_count().

    cores_per_worker: optional float
      number of cores to be allocated to each worker

    configured_max_workers_per_node: optional int
      user specific maximum of number of workers per node

    accelerators: optional int
      number of accelerators per node
    """

    # Each possible limit can add an additional upper bound into this set.
    # At the end, this function will compute the minimum/least upper bound.
    upper_bounds: set[int] = set()

    logger.debug("Calculating maximum workers per node")

    if configured_max_workers_per_node:
        logger.debug("Adding upper bound %s due to configured max workers per node", configured_max_workers_per_node)
        upper_bounds.add(configured_max_workers_per_node)

    if cores_per_node and cores_per_worker:
        core_slots = math.floor(cores_per_node / cores_per_worker)
        logger.debug("Adding upper bound %s due to %s cores per node, %s cores per worker", core_slots, cores_per_node, cores_per_worker)
        upper_bounds.add(core_slots)

    if mem_per_node and mem_per_worker:
        mem_slots = math.floor(mem_per_node / mem_per_worker)
        logger.debug("Adding upper bound %s due to %s GB memory per node, %s GB memory per worker", mem_slots, mem_per_node, mem_per_worker)
        upper_bounds.add(mem_slots)

    num_accelerators = len(accelerators)
    if num_accelerators > 0:
        logger.debug("Adding upper bound %s due to configured accelerators", num_accelerators)
        upper_bounds.add(num_accelerators)

    if upper_bounds:
        m = min(upper_bounds)
        logger.debug("Calculated maximum workers per node: %s", m)
        return m
    else:
        logger.debug("Calculated no maximum workers per node")
        return None
