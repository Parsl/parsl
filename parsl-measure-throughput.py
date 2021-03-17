import concurrent.futures
import parsl
import time

@parsl.python_app
def task(duration: int, parsl_resource_specification={}):
    import time
    time.sleep(duration)

if __name__ == "__main__":
    print("parsl-measure-throughput: start")

    print("parsl-measure-throughput: importing config")
    # from parsl.tests.configs.local_threads import config
    from confwq_mon import config
    # from confwq import config

    print("parsl-measure-throughput: initialising ")
    parsl.load(config)
 
    n = 68 * 4 * 4 * 2 #  68 cores, 4 threads/core, 4 nodes, 2 batches
    d = 600
    print(f"parsl-measure-throughput: submitting {n} tasks of duration {d} seconds")

    start_time = time.time()

    futures = [task(d, parsl_resource_specification={'cores' :1, 'memory':0, 'disk':0}) for _ in range(0,n)]

    print("parsl-measure-throughput: waiting for all futures")

    count = 0
    for f in concurrent.futures.as_completed(futures):
        count += 1
        print(f"{count} futures completed after {time.time() - start_time} seconds")

    end_time = time.time()

    print(f"parsl-measure-throughput: duration was {end_time - start_time} seconds")
    print(f"parsl-measure-throughput: concurrency was {(n * d) / (end_time - start_time)}")

    print("parsl-measure-throughput: stopping parsl")

    parsl.dfk().cleanup()
    parsl.clear()

    print("parsl-measure-throughput: end")
