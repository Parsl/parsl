import parsl
from parsl import App, DataFlowKernel
from parsl.monitoring.db_logger import MonitoringConfig

threads_config = parsl.config.Config(executors=[parsl.executors.threads.ThreadPoolExecutor(label='threads', max_threads=4)],
                                     monitoring_config=MonitoringConfig(database_type='local_database', logger_name='parsl_db_logger',
                                     eng_link='sqlite:///parsl.db', web_app_host='http://localhost', web_app_port=8899, resource_loop_sleep_duration=15))
dfk = DataFlowKernel(config=threads_config)


@App('python', dfk)
def cpu_stress_fail(workers=1, timeout=10, inputs=[], stdout='stdout_for_fail.txt', stderr='stderr_for_fail.txt'):
    raise AssertionError("Just an Error")
    cpu_stress()


@App('python', dfk)
def cpu_stress(workers=1, timeout=10, inputs=[], outputs=[]):
    s = 0
    for i in range(10**8):
        s += i
    return s


if __name__ == "__main__":
    MAXTIMEOUT = 200
    # MAXTIMEOUT = 50
    MINTIMEOUT = 60

    from random import randrange
    r = randrange

    a1, b1 = [cpu_stress(workers=1, timeout=r(MINTIMEOUT, MAXTIMEOUT)),
              cpu_stress(workers=1, timeout=r(MINTIMEOUT, MAXTIMEOUT))]
    a1.result()
    b1.result()

    ins = [cpu_stress(workers=1, timeout=r(MINTIMEOUT, MAXTIMEOUT), inputs=[a1]),
           cpu_stress(workers=1, timeout=r(MINTIMEOUT, MAXTIMEOUT), inputs=[b1]),
           cpu_stress(workers=1, timeout=r(MINTIMEOUT, MAXTIMEOUT), inputs=[b1]),
           cpu_stress(workers=1, timeout=r(MINTIMEOUT, MAXTIMEOUT), inputs=[b1])]

    a3 = cpu_stress(workers=1, timeout=r(MINTIMEOUT, MAXTIMEOUT), inputs=[*ins])
    print(a3.result())
    a4 = cpu_stress_fail(workers=1, timeout=r(MINTIMEOUT, MAXTIMEOUT), inputs=[a1])

    dfk.cleanup()
