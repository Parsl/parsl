from enum import IntEnum

class States(IntEnum):
    ''' Map states for tasks to an Int
    '''
    unsched  = -1
    pending  = 0
    runnable = 1
    running  = 2
    done     = 3
    failed   = 4
    dep_fail = 5


if __name__ == "__main__":
    print(States.pending)
    print(States.done)
    print(3 == States.done)
