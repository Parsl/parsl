#!/usr/bin/env python3

from parsl import *

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(workers, lazy_fail=True)

@App('python', dfk)
def sleep_fail(sleep_dur, sleep_rand_max, fail_prob, inputs=[]):
    import time
    import random

    s = sleep_dur + random.randint(-sleep_rand_max, sleep_rand_max)
    print("Sleeping for : ", s)
    time.sleep(s)
    x = float(random.randint(0,100)) / 100
    if x <= fail_prob :
        print("Fail")
        raise Exception("App failure")
    else:
        print("Succeed")



def test1 (numtasks ) :
    fus = []
    for i in range(0,10):

        fu = sleep_fail(0, 0, .8)
        fus.extend([fu])


    count = 0
    for fu in fus :
        try:
            x = fu.result()
        except Exception as e:
            count += 1
            print("Caught fail ")

    print("Caught failures of  {0}/{1}".format(count, len(fus)))


def test_deps (numtasks) :

    '''
    App1   App2  ... AppN
    '''
    fus = []
    for i in range(0,10):
        fu = sleep_fail(1, 0, .8)
        fus.extend([fu])

    '''
    App1   App2  ... AppN
    |       |        |
    V       V        V
    App1   App2  ... AppN
    '''

    fus_2 = []
    for fu in fus:
        fu = sleep_fail(0, 0, .8, inputs=[fu])
        fus_2.extend([fu])

    '''
    App1   App2  ... AppN
      |       |        |
      V       V        V
    App1   App2  ... AppN
       \      |       /
        \     |      /
          App_Final
    '''
    fu_final = sleep_fail(1, 0, 0, inputs=fus_2)


    print("Final status : ", fu_final.result())


if __name__ == "__main__" :

    test_deps(10)
