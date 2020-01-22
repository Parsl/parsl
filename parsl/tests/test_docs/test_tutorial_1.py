import argparse
import parsl

import pytest

from parsl.app.app import bash_app
from parsl.tests.configs.local_threads import config


local_config = config


@bash_app
def sim_mol_dyn(i, dur, outputs=[], stdout=None, stderr=None):
    # The bash app function, requires that the bash script is assigned to the special variable
    # cmd_line. Positional and Keyword args to the fn() are formatted into the cmd_line string
    cmd_line = """echo "{0}" > {outputs[0]}
    sleep {1};
    ls ;
    """
    return cmd_line


@pytest.mark.skip('hangs in pytest')
def test_data_future_result():
    """Testing the behavior of a result call on DataFutures
    """
    # We call sim_mol_dyn with
    sim_fut = sim_mol_dyn(5, 0, outputs=['sim.out'],
                          stdout='stdout.txt', stderr='stderr.txt')
    data_futs = sim_fut.outputs
    print("Launching and waiting on data_futs")
    print("Done?   : ", data_futs[0].done())
    print("Result? : ", data_futs[0].result(timeout=1))


@pytest.mark.skip('hangs in pytest')
def test_app_future_result():
    """Testing the behavior of a result call on AppFutures
    """
    # We call sim_mol_dyn with
    sim_fut = sim_mol_dyn(5, 0.5, outputs=['sim.out'],
                          stdout='stdout.txt', stderr='stderr.txt')
    sim_fut.outputs
    print("Launching and waiting on data_futs")
    print("Done?   : ", sim_fut.done())
    print("Result? : ", sim_fut.result(timeout=1))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_data_future_result()
