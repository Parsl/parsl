# benchmarking of large serialization values

import time
import random

from parsl.serialize import serialize as serializer
from parsl.serialize import deserialize as deserializer

iterations = 100

for large_count in [10, 100, 1000, 10000, 100000, 1000000, 10000000]:

    serialized_v = {}
    for n in range(0, iterations):
        large_v = [random.randint(0, 1000000) for _ in range(large_count)]
        serialized_v[n] = serializer(large_v)

    print("")
    print("--")

    print("test using same list object every time:")
    print(f"large_count = {large_count}")

    t_start = time.time()
    for n in range(0, iterations):
        deserializer(serialized_v[n])
    t_end = time.time()

    print(f"time for deserialize all ns loop: {t_end - t_start}s")

    t_per_iter = (t_end - t_start) / iterations
    print(f"time per iteration: {t_per_iter}s")

    t_per_element = t_per_iter / large_count
    print(f"time per list element: {t_per_element}s")
