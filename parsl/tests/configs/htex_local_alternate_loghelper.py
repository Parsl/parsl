
def my_log_handler(rundir, name):
    import logging
    import os

    import parsl.log_utils as lu
    os.makedirs(rundir, exist_ok=True)
    callback = lu.set_json_file_logger(f"{rundir}/{name}.jsonlog", name="", level=logging.DEBUG)
    logging.info(f"This is the test my_log_handler logging JSON for component {name}, logging into {rundir}")
    # TODO: can I add component and rundir fields into every log message to help disambiguate?
    return callback
