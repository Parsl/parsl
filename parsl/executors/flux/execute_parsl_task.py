"""Script for executing tasks inside of Flux jobs."""

import argparse
import os
import logging

from parsl.executors.high_throughput.process_worker_pool import execute_task
from parsl.serialize import serialize
from parsl.executors.flux import TaskResult


def main():
    """Execute one rank of an MPI application."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True, help="Input pickle file")
    parser.add_argument("-o", "--output", required=True, help="Output pickle file")
    args = parser.parse_args()
    logging.info("Input : %s", args.input)
    logging.info("Output : %s", args.output)
    returnval = None
    exception = None
    # open and deserialize the task's pickled input package
    with open(args.input, "rb") as file_handle:
        fn_buf = file_handle.read()
    logging.info("Read input pickle file")
    try:
        returnval = execute_task(fn_buf)
    except Exception as exc:
        logging.exception("Parsl task execution failed:")
        exception = exc
    else:
        logging.info("Finished execution")
    # only rank 0 should write/return a result; other ranks exit
    if int(os.environ["FLUX_TASK_RANK"]) == 0:
        # write the result to the output file
        result_buf = serialize(TaskResult(returnval, exception))
        with open(args.output, "wb") as file_handle:
            file_handle.write(result_buf)


if __name__ == "__main__":
    main()
