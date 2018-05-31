import os
import uuid
from parsl import DataFlowKernel, App
from parsl import set_stream_logger
# set_stream_logger()
base_config = {
    "sites": [
        {
            "site": "local_ipp",
            "auth": {
                "channel": None
            },
            "execution": {
                "executor": "ipp",
                "provider": "local",
                "block": {
                    "initBlocks": 3,
                }
            }
        }
    ],
    "globals": {
        "lazyErrors": True,
        "retries": 2
    }
}


def test_manual_scale_in():
    config = base_config.copy()
    config['globals']['strategy'] = None
    dfk = DataFlowKernel(config=config)
    exc = dfk.executors['local_ipp']

    @App('python', dfk, walltime=10)
    def first():
        import time
        time.sleep(1)
        return 'first'

    @App('python', dfk, walltime=10)
    def second():
        import time
        time.sleep(4)
        return 'second'

    @App('python', dfk, walltime=10)
    def third():
        import time
        time.sleep(3)
        return 'third'

    # print(exc.executor.queue_status())

    first_fut, second_fut, third_fut = first(), second(), third()
    assert first_fut.result() == 'first'
    print(exc.executor.queue_status())
    exc.scale_in(1)

    print(exc.executor.queue_status())
    assert second_fut.result() == 'second'
    assert third_fut.result() == 'third'


def test_auto_scale_in():
    config = base_config.copy()
    config['sites'][0]['execution']['block']['initBlocks'] = 1
    config['sites'][0]['execution']['block']['maxBlocks'] = 3
    dfk = DataFlowKernel(config=config)

    @App('python', dfk)
    def small_record(*args, **kwargs):
        import random
        import time
        total_seconds = random.randint(3, 10)
        with open(kwargs['record_filename'], 'a') as record:
            record.write('{}\n'.format(total_seconds))
            for i in range(total_seconds):
                time.sleep(1)
                record.write('{}\n'.format(i + 1))

    # Round one of auto scale in
    filename_prefix = str(uuid.uuid4())[:8]
    futs = [small_record(record_filename='{}_{}.txt'.format(filename_prefix, i)) for i in range(8)]
    for fut in futs:
        fut.result()

    # Check results of round one
    # This ensures that none of the tasks had their engines killed mid-run
    for record_i in range(8):
        record_filename = '{}_{}.txt'.format(filename_prefix, record_i)
        record = open(record_filename).readlines()
        assert record[0].strip() == record[-1].strip()
        os.remove(record_filename)

    # Round two of auto scale in
    filename_prefix2 = str(uuid.uuid4())[:8]
    futs2 = [small_record(record_filename='{}_{}.txt'.format(filename_prefix2, i)) for i in range(8)]
    for fut in futs2:
        fut.result()

    # Check results of round two
    for record_i in range(8):
        record_filename = '{}_{}.txt'.format(filename_prefix2, record_i)
        record = open(record_filename).readlines()
        assert record[0].strip() == record[-1].strip()
        os.remove(record_filename)

# test_manual_scale_in()
test_auto_scale_in()
