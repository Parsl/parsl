import argparse
import logging

import parsl

parsl.load()

from parsl import python_app


@python_app
def platform(sleep=10, stdout=None):
    import platform
    import time
    time.sleep(sleep)
    return platform.uname()


class SkipTasksFilter(logging.Filter):

    def __init__(self, avoid_string):
        self.avoid = avoid_string

    def filter(self, record):
        return self.avoid not in record.getMessage()


def test_platform(n=2):
    # sync
    logger = logging.getLogger("parsl.dataflow.dflow")
    skip_tags = ['Task', 'dependencies']
    for skip in skip_tags:
        skip_filter = SkipTasksFilter(skip)
        logger.addFilter(skip_filter)

    x = platform(sleep=0)
    print(x.result())

    d = []
    for i in range(0, n):
        x = platform(sleep=5)
        d.append(x)

    print(set([i.result()for i in d]))

    dfk = parsl.dfk()
    dfk.cleanup()

    with open("{}/parsl.log".format(dfk.run_dir)) as f:

        for line in f.readlines():
            if any(skip in line for skip in skip_tags):
                raise Exception("Logline {} contains a skip tag".format(line))
    return True


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sitespec", default=None)
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.sitespec:
        c = None
        try:
            exec("import parsl; from {} import config".format(args.sitespec))
            parsl.load(c)
        except Exception:
            print("Failed to load the requested config : ", args.sitespec)
            exit(0)

    if args.debug:
        parsl.set_stream_logger()

    x = test_platform()
