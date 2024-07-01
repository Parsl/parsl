from config import htex_config
from library import increment

import parsl

parsl.load(htex_config)

for i in range(5):
    print('{} + 1 = {}'.format(i, increment(i).result()))
