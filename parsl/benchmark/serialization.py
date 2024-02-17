import time
import parsl.serialize as s
import dill
import pickle


def prof(serializer, deserializer):
    tot_iters = 100000

    t_start = time.time()
    for _ in range(0, tot_iters):
        pass
    t_end = time.time()

    print(f"time for null loop: {t_end - t_start}s")
    print(f"time per iteration: {(t_end - t_start) / tot_iters}s")

    t_start = time.time()
    for _ in range(0, tot_iters):
        serializer(7)
    t_end = time.time()

    print(f"time for serialize 7 loop: {t_end - t_start}s")
    print(f"time per iteration: {(t_end - t_start) / tot_iters}s")

    t_start = time.time()
    for n in range(0, tot_iters):
        serializer(n)
    t_end = time.time()

    print(f"time for serialize all ns loop: {t_end - t_start}s")
    print(f"time per iteration: {(t_end - t_start) / tot_iters}s")

    t_start = time.time()
    for _ in range(0, tot_iters):
        serializer("hello")
    t_end = time.time()

    print(f"time for serialize hello loop: {t_end - t_start}s")
    print(f"time per iteration: {(t_end - t_start) / tot_iters}s")

    def f():
        """This is a test function to be serialized"""
        return 100

    try:
        t_start = time.time()
        for _ in range(0, tot_iters):
            serializer(f)
        t_end = time.time()

        print(f"time for serialize f loop: {t_end - t_start}s")
        print(f"time per iteration: {(t_end - t_start) / tot_iters}s")
    except Exception as e:
        print(f"Exception in serialize f loop: {e}")

    t_start = time.time()
    for n in range(0, tot_iters):
        deserializer(serializer(n))
    t_end = time.time()

    print(f"time for serialize/deserialize all ns loop: {t_end - t_start}s")
    print(f"time per iteration: {(t_end - t_start) / tot_iters}s")


if __name__ == "__main__":
    print("parsl serialization benchmark")
    print("parsl.serialize:")
    prof(s.serialize, s.deserialize)
    print("dill.dumps:")
    prof(dill.dumps, dill.loads)
    print("pickle.dumps:")
    prof(pickle.dumps, pickle.loads)
