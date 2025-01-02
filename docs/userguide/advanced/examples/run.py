from argparse import ArgumentParser

import parsl

from library.config import make_local_config
from library.app import convert_many_to_binary
from parsl.app.python import PythonApp

# Protect the script from running twice.
#  See "Safe importing of main module" in Python multiprocessing docs
#  https://docs.python.org/3/library/multiprocessing.html#multiprocessing-programming
if __name__ == "__main__":
    # Get user instructions
    parser = ArgumentParser()
    parser.add_argument('--numbers-per-batch', default=8, type=int)
    parser.add_argument('numbers', nargs='+', type=int)
    args = parser.parse_args()

    # Prepare the workflow functions
    convert_app = PythonApp(convert_many_to_binary, cache=False)

    # Load the configuration
    #  As a context manager so resources are shutdown on exit
    with parsl.load(make_local_config()):

        # Spawn tasks
        futures = [
            convert_app(args.numbers[start:start + args.numbers_per_batch])
            for start in range(0, len(args.numbers), args.numbers_per_batch)
        ]

        # Retrieve task results
        for future in futures:
            for x, b in zip(future.task_record['args'][0], future.result()):
                print(f'{x} -> {"".join("1" if i else "0" for i in b)}')
