import pytest
import os
import shutil
import time

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.monitoring import MonitoringHub
from parsl.providers import LocalProvider
from parsl.data_provider.files import File


@parsl.bash_app
def initialize(outputs=[]):
    import time
    time.sleep(1)
    return f"echo 'Initialized' > {outputs[0]}"


@parsl.python_app
def split_data(inputs=None, outputs=None):
    with open(inputs[0], 'r') as fh:
        data = fh.read()
    for i, op in enumerate(outputs):
        with open(op, 'w') as ofh:
            ofh.write(f"{i + 1}\n")
            ofh.write(data)


@parsl.python_app
def process(inputs=None, outputs=None):
    with open(inputs[0], 'r') as fh:
        data = fh.read()
    with open(outputs[0], 'w') as ofh:
        ofh.write(f"{data} processed")


@parsl.python_app
def combine(inputs=None, outputs=None):
    with open(outputs[0], 'w') as ofh:
        for fl in inputs:
            with open(fl, 'r') as fh:
                ofh.write(fh.read())
                ofh.write("\n")


def fresh_config(run_dir):
    return Config(
        run_dir=str(run_dir),
        executors=[
            HighThroughputExecutor(
                address="127.0.0.1",
                label="htex_local",
                provider=LocalProvider(
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                )
            )
        ],
        strategy='simple',
        strategy_period=0.1,
        monitoring=MonitoringHub(
                        hub_address="localhost",
                        logging_endpoint=f"sqlite:///{run_dir}/monitoring.db",
                        file_provenance=True,
        )
    )


@pytest.mark.local
def test_provenance(tmpd_cwd):
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy

    cfg = fresh_config(tmpd_cwd)
    my_dfk = parsl.load(cfg)

    cwd = os.getcwd()
    if os.path.exists(os.path.join(cwd, 'provenance')):
        shutil.rmtree(os.path.join(cwd, 'provenance'))
    os.mkdir(os.path.join(cwd, 'provenance'))
    os.chdir(os.path.join(cwd, 'provenance'))

    my_dfk.log_info("Starting Run")

    init = initialize(outputs=[File(os.path.join(os.getcwd(), 'initialize.txt'))])

    sd = split_data(inputs=[init.outputs[0]],
                    outputs=[File(os.path.join(os.getcwd(), f'split_data_{i}.txt')) for i in range(4)])

    p = [process(inputs=[sdo], outputs=[File(os.path.join(os.getcwd(), f'processed_data_{i}.txt'))]) for i, sdo in
         enumerate(sd.outputs)]

    c = combine(inputs=[pp.outputs[0] for pp in p], outputs=[File(os.path.join(os.getcwd(), 'combined_data.txt'))])

    c.result()
    os.chdir(cwd)
    my_dfk.log_info("Ending run")
    time.sleep(2)

    engine = sqlalchemy.create_engine(cfg.monitoring.logging_endpoint)
    with engine.begin() as connection:
        def count_rows(table: str):
            result = connection.execute(f"SELECT COUNT(*) FROM {table}")
            (c,) = result.first()
            return c

        assert count_rows("workflow") == 1

        assert count_rows("task") == 7

        assert count_rows("files") == 10

        assert count_rows("input_files") == 9

        assert count_rows("output_files") == 10

        assert count_rows("misc_info") == 2

    parsl.clear()
