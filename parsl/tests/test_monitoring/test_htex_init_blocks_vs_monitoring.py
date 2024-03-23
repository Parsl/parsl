import logging
import os
import parsl
import pytest
import time

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SimpleLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.monitoring import MonitoringHub


def fresh_config(run_dir, strategy, db_url):
    return Config(
        run_dir=os.fspath(run_dir),
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                cores_per_worker=1,
                encrypted=True,
                provider=LocalProvider(
                    channel=LocalChannel(),
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
                        hub_port=55055,
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
    parsl.load(fresh_config(tmpd_cwd, strategy, db_url))

    this_app().result()

    parsl.dfk().cleanup()
    parsl.clear()

    engine = sqlalchemy.create_engine(db_url)
    with engine.begin() as connection:

        result = connection.execute(text("SELECT COUNT(DISTINCT block_id) FROM block"))
        (c, ) = result.first()
        assert c == 1, "We should see a single block in this database"

        result = connection.execute(text("SELECT COUNT(*) FROM block WHERE block_id = 0 AND status = 'PENDING'"))
        (c, ) = result.first()
        assert c == 1, "There should be a single pending status"

        result = connection.execute(text("SELECT COUNT(*) FROM block WHERE block_id = 0 AND status = 'CANCELLED'"))
        (c, ) = result.first()
        assert c == 1, "There should be a single cancelled status"
