from parsl.tests.configs.workqueue_monitoring import fresh_config

# this is a separate file so that it can be imported only
# when used with the whole test suite vs workqueue

# otherwise, attempting to import the workqueue_monitoring
# module fails if workqueue isnt' around.

# there might be a better way to do this that looks like how other
# stuff is done

config = fresh_config()
