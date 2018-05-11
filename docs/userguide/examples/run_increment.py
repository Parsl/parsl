import parsl
from config import local_threads
from library import increment

parsl.load(local_threads)

for i in range(5):
    print('{} + 1 = {}'.format(i, increment(i).result()))

