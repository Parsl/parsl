
def my_log_handler(rundir, name):
    import logging
    import os

    import parsl.log_utils as lu
    os.makedirs(rundir, exist_ok=True)
    callback = lu.set_file_logger(f"{rundir}/{name}.log", name="", level=logging.DEBUG)
    logging.info(f"This is the test my_log_handler for name {name}, logging into {rundir}")
    return callback
