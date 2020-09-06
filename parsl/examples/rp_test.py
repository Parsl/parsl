# This example should be removed from the main directory.

import logging
import os

from radical.pilot import PilotManager, Session, ComputePilotDescription, UnitManager

from parsl.config import Config
from parsl.executors.radical_pilot.executor import RadicalPilotExecutor

logger = logging.getLogger(__name__)

import parsl
from parsl.app.app import python_app


os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://localhost/radical'

session = Session()

pmgr = PilotManager(session=session)

pdesc = ComputePilotDescription({
        'resource': 'local.localhost',
        'runtime': 60,
        'cores': 1
})
umgr = UnitManager(session=session)

for i in range(2):
    pilot = pmgr.submit_pilots(pdesc)
    umgr.add_pilots(pilot)


conf = Config(
    executors=[
        RadicalPilotExecutor(
            unit_manager=umgr,
            keep_task_dir=True,
            keep_failed_task_dir=True
        )
    ],
    strategy=None,
)
parsl.load(conf)


def local_teardown():
    parsl.clear()


@python_app
def add(a: int, b: int):
    return a + b


r = add(2, 3)

print(r.result())
