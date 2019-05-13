import argparse
import parsl
from parsl.data_provider.files2 import File

from parsl.app.app import python_app, bash_app

from parsl.configs.htex_local import config
from functools import wraps
parsl.load(config)

# flake8: noqa

def file_staging_wrapper(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        from parsl.data_provider.files import File
        # import logging
        logger.info("[file_staging_wrapper] Args: {}".format(args))
        logger.info("[file_staging_wrapper] Task_id: {}".format(parsl_task_id))     # Injected as global
        logger.info("[file_staging_wrapper] Sandbox: {}".format(parsl_sandbox_dir)) # Injected as global
        inputs = kwargs.get('inputs', [])
        outputs = kwargs.get('outputs', [])
        new_args = []
        for arg in args:
            # item = None
            if isinstance(arg, File):
                logger.info("[file_staging_wrapper] Found {}".format(arg))
                new_args.append(File.filepath)
            else:
                new_args.append(arg)

        for arg in inputs:
            logger.info("[file_staging_wrapper] input:{}".format(arg))
            arg.stage_in(dest_dir=parsl_sandbox_dir)

        retval = func(*args, **kwargs)

        for arg in outputs:
            logger.info("[file_staging_wrapper] output:{}".format(arg))

        return retval

    return wrapper


@python_app
@file_staging_wrapper
def py_cat(msg, outputs=None):
    with open(str(outputs[0]), 'w') as f:
        f.write(msg)


@bash_app
@file_staging_wrapper
def cat(msg, inputs=None, outputs=None):
    return "cat {0} {2}; echo '{1}' >> {2}".format(inputs[0], msg, outputs[0])


@bash_app
@file_staging_wrapper
def concat(inputs=None, outputs=None):
    return "cat {0} {1} >> {2}".format(inputs[0], inputs[1], outputs[0])


def call_sleep(size):
    x = py_cat("Hello 0", outputs=['py_cat.txt'])
    y1 = cat("Hi 0", inputs=x.outputs, outputs=['ba_cat_1.txt'])
    y2 = cat("Hi 0", inputs=x.outputs, outputs=['ba_cat_1.txt'])
    z = concat(inputs=[y1.outputs[0], y2.outputs[0]], outputs=['concat.txt'])

    print(z.result())

@python_app
@file_staging_wrapper
def aggregate(inputs=None):
    with open(inputs[0]) as f:
        return sum([int(line.strip()) for line in f.readlines()])


def http_test():

    f = File("https://gist.githubusercontent.com/yadudoc/7f21dd15e64a421990a46766bfa5359c/raw/7fe04978ea44f807088c349f6ecb0f6ee350ec49/unsorted.txt")
    x = aggregate(inputs=[f])
    print("Got result : {}".format(x.result()))

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--count", default="3",
                        help="Count of apps to launch")

    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")

    args = parser.parse_args()
    parsl.set_stream_logger()

    # call_sleep(int(args.count))
    http_test()
