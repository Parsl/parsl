import logging
import os
import time

import pytest

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.monitoring import MonitoringHub
from parsl.providers import LocalProvider


def fresh_config(run_dir, strategy, db_url):
    return Config(
        run_dir=os.fspath(run_dir),
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                cores_per_worker=1,
                encrypted=True,
                provider=LocalProvider(
                    init_blocks=1,
                    # min and max are set to 0 to ensure that we don't get
                    # a block from ongoing strategy scaling, only from
                    # init_blocks
                    min_blocks=0,
                    max_blocks=0,
                    launcher=SimpleLauncher(),
                ),
            )
        ],
        strategy=strategy,
        strategy_period=0.1,
        monitoring=MonitoringHub(
                        hub_address="localhost",
                        logging_endpoint=db_url
        )
    )


@parsl.python_app
def this_app():
    pass


@pytest.mark.local
@pytest.mark.parametrize("strategy", ('none', 'simple', 'htex_auto_scale'))
def test_row_counts(tmpd_cwd, strategy):
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from sqlalchemy import text

    db_url = f"sqlite:///{tmpd_cwd}/monitoring.db"
    with parsl.load(fresh_config(tmpd_cwd, strategy, db_url)):
        dfk = parsl.dfk()
        run_id = dfk.run_id

        this_app().result()

    engine = sqlalchemy.create_engine(db_url)
    with engine.begin() as connection:

        binds = {"run_id": run_id}

        result = connection.execute(text("SELECT COUNT(DISTINCT block_id) FROM block WHERE run_id = :run_id"), binds)
        (c, ) = result.first()
        assert c == 1, "We should see a single block in this database"

        result = connection.execute(text("SELECT COUNT(*) FROM block WHERE block_id = 0 AND status = 'PENDING' AND run_id = :run_id"), binds)
        (c, ) = result.first()
        assert c == 1, "There should be a single pending status"

        result = connection.execute(text("SELECT COUNT(*) FROM block WHERE block_id = 0 AND status = 'SCALED_IN' AND run_id = :run_id"), binds)
        (c, ) = result.first()
        assert c == 1, "There should be a single cancelled status"
