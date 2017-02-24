'''
DataFlowKernel
==============

The DataFlowKernel adds dependency awareness to an existing executor.
It is responsible for managing futures, such that when dependencies are resolved, pending tasks
move to the runnable state.

Here's a simplified diagram of what happens internally::

    User             |        DFK         |    Executor
    ----------------------------------------------------------
                     |                    |
          Task-------+> +Submit           |
        App_Fu<------+--|                 |
                     |  Dependencies met  |
                     |         task-------+--> +Submit
                     |        Ex_Fu<------+----|
'''

import copy
import uuid
from functools import partial
from parsl.dataflow.error import *
import logging
from concurrent.futures import Future
from parsl.dataflow.futures import AppFuture
from inspect import signature

# Exceptions

logger = logging.getLogger(__name__)

class DataFlowKernel(object):
    """ DataFlowKernel
    """

    def __init__(self, executor):
        """ Initialize the DataFlowKernel
        Args:
            executor (Executor): An executor object.

        Returns:
            DataFlowKernel object
        """
        self.pending         = {}
        self.fut_task_lookup = {}
        self.runnable        = {}
        self.done            = {}
        self.executor        = executor

    @staticmethod
    def _count_deps(depends, task_id):
        ''' Internal. Count the number of unresolved futures in the list depends'''

        count = 0
        for dep in depends:
            if isinstance(dep, Future) or issubclass(type(dep), Future):
                if not dep.done():
                    count += 1

        logger.debug("Task:{0}   dep_cnt:{1}  deps:{2}".format(task_id, count, depends))
        return count

    def handle_update(self, task_id, future):
        ''' This function is called only as a callback from a task being done
        Move done task from runnable -> done
        Move newly doable tasks from pending -> runnable , and launch

        Args:
             task_id (string) : Task id which is a uuid string
             future (Future) : The future object corresponding to the task which makes this callback
        '''

        if future.done():
            logger.debug("Completed : %s with %s", task_id, future)
        else:
            logger.debug("Failed    : %s with %s", task_id, future)

        # Identify tasks that have resolved dependencies and launch
        to_delete = []
        for task in list(self.pending.keys()):
            if self._count_deps(self.pending[task]['depends'], task) == 0 :
                # We can now launch *task*
                logger.debug("Task : %s is now runnable", task)
                try:
                    self.runnable[task] = self.pending[task]
                except KeyError:
                    continue

                to_delete.extend([task])
                self.runnable[task]['executable'] = self.sanitize_and_wrap(task_id,
                                                                           self.pending[task]['func'],
                                                                           self.pending[task]['args'],
                                                                           self.pending[task]['kwargs'])
                exec_fu = self.launch_task(self.runnable[task]['executable'], task)
                logger.debug("Updating parent of %s to %s", self.runnable[task]['app_fu'],
                             exec_fu)
                # Update the App_future with the Future from launch.
                self.runnable[task]['app_fu'].update_parent(exec_fu)
                self.runnable[task]['exec_fu'] = exec_fu

        #logger.debug("Deleteing : %s", to_delete)
        for task in to_delete:
            try:
                del self.pending[task]
            except KeyError:
                logger.debug("Caught KeyError on %s deleting from pending", task)
                pass

        # Move the done task from runnable to done
        try:
            self.done[task_id] = self.runnable[task_id]
            del self.runnable[task_id]
        except KeyError:
            logger.debug("Caught KeyError on %s deleting from runnable", task_id)
        finally:
            self.write_status_log()

        return


    def write_status_log(self):
        ''' Write status log.

        Args:
           None

        Kwargs:
           None
        '''
        logger.debug("Pending:%d   Runnable:%d   Done:%d", len(self.pending),
                     len(self.runnable), len(self.done))

    def print_status_log(self):
        ''' Print status log in terms of pending, runnable and done tasks

        Args:
           None

        Kwargs:
           None
        '''
        print("Pending:{0}   Runnable:{1}   Done:{2}".format( len(self.pending),
                                                              len(self.runnable),
                                                              len(self.done)) )

    def launch_task(self, executable, task_id):
        ''' Handle the actual submission of the task to the executor layer

        We should most likely add a callback at this point

        Args:
            executable (callable) : A callable object
            task_id (uuid string) : A uuid string that uniquely identifies the task

        Returns:
            Future that tracks the execution of the submitted executable
        '''

        logger.debug("Submitting to executor : %s", task_id)
        exec_fu = self.executor.submit(executable)
        exec_fu.add_done_callback(partial(self.handle_update, task_id))
        return exec_fu

    @staticmethod
    def _count_all_deps(task_id, args, kwargs):
        ''' Internal. Count the number of unresolved futures in the list depends
        Args:
            task_id (uuid string) : Task_id
            args (List[args]) : The list of args list to the fn
            kwargs (Dict{kwargs}) : The dict of all kwargs passed to the fn

        Returns:
            count, [list of dependencies]

        '''

        # Check the positional args
        depends = []
        count   = 0
        for dep in args :
            if isinstance(dep, Future) or issubclass(type(dep), Future):
                if not dep.done():
                    count += 1
                    depends.extend([dep])

        # Check for explicit kwargs ex, fu_1=<fut>
        for key in kwargs:
            dep = kwargs[key]
            if isinstance(dep, Future) or issubclass(type(dep), Future):
                if not dep.done():
                    count += 1
                    depends.extend([dep])

        # Check for futures in inputs=[<fut>...]
        for dep in kwargs.get('inputs', []):
            dep = kwargs[key]
            if isinstance(dep, Future) or issubclass(type(dep), Future):
                if not dep.done():
                    count += 1
                    depends.extend([dep])

        logger.debug("Task:{0}   dep_cnt:{1}  deps:{2}".format(task_id, count, depends))
        return count, depends

    @staticmethod
    def sanitize_and_wrap(task_id, func, args, kwargs):
        ''' This function should be called **ONLY** when all the futures we track
        have been resolved. If the user hid futures a level below, we will not catch
        it, and will (most likely) result in a type error .

        Args:
             task_id (uuid str) : Task id
             func (Function) : App function
             args (List) : Positional args to app function
             kwargs (Dict) : Kwargs to app function

        Return:
             partial Function evaluated with all dependencies in  args, kwargs and
        kwargs['inputs'] evaluated.
        '''
        # Todo: This function is not tested.
        logger.debug("%s Sanitizing %s %s", task_id, args, kwargs)

        # Replace item in args
        new_args = []
        for dep in args :
            if isinstance(dep, Future) or issubclass(type(dep), Future):
                new_args.extend([dep.result()])
            else:
                new_args.extend([dep])

        # Check for explicit kwargs ex, fu_1=<fut>
        for key in kwargs:
            dep = kwargs[key]
            if isinstance(dep, Future) or issubclass(type(dep), Future):
                kwargs[key] = dep.result()

        # Check for futures in inputs=[<fut>...]
        if 'inputs' in kwargs:
            new_inputs = []
            for dep in kwargs['inputs']:
                #dep = kwargs['inputs']
                if isinstance(dep, Future) or issubclass(type(dep), Future):
                    new_inputs.extend([dep.result()])
                else:
                    new_inputs.extend([dep])
            kwargs['inputs'] = new_inputs

        return partial(func, *new_args, **kwargs)


    def submit (self, executable, depends, callback):
        ''' Add task to the dataflow system.

        If all deps are met :
              send to the runnable queue
              and launch the task
        Else:
              post the task in the pending queue

        Returns:
               (AppFuture) [DataFutures,]
        '''

        _func, _args, _kwargs = executable

        task_id  = uuid.uuid4()
        dep_cnt, depends = self._count_all_deps(task_id, _args, _kwargs)

        #dep_cnt  = self._count_deps(depends, task_id)
        task_def = { 'depends'    : depends,
                     'func'       : _func,
                     'args'       : _args,
                     'kwargs'     : _kwargs,
                     'callback'   : callback,
                     'dep_cnt'    : dep_cnt,
                     'exec_fu'    : None,
                     'app_fu'     : None  }

        if dep_cnt == 0 :
            # Send to runnable queue
            if task_id in self.runnable:
                raise DuplicateTaskError("Task {0} in pending list".format(task_id))

            else:
                self.runnable[task_id] = task_def

            executable = self.sanitize_and_wrap(task_id, _func, _args, _kwargs)
            task_def['exec_fu']    = self.launch_task(executable, task_id)
            task_def['app_fu']     = AppFuture(task_def['exec_fu'])
            self.runnable[task_id] = task_def

        else:
            # Send to pending queue
            if task_id in self.pending:
                raise DuplicateTaskError("Task {0} in pending list".format(task_id))

            else:
                task_def['app_fu'] = AppFuture(None)
                self.pending[task_id] = task_def

        for fut in depends:
            if fut not in self.fut_task_lookup:
                self.fut_task_lookup[fut] = []
                self.fut_task_lookup[fut].extend([task_id])

        logger.debug("Launched : %s with %s", task_id, task_def['app_fu'])
        return task_def['app_fu']


    def check_fulfilled(self):
        ''' Iterate over the pending tasks
        For tasks with dep_cnt == 0, copy task to runnable, and make the callback
        '''

        runnable = []
        for task in self.pending:
            if self.pending[task]['dep_cnt'] == 0:
                print("All deps resolved for : ", task, self.pending[task])
                self.runnable[task] = copy.deepcopy ( self.pending[task] )

                runnable.extend([task])

                print("Running callback {0}".format(task))
                self.runnable[task]['callback']

        for task in runnable:
            del self.pending[task]
        return




def cbk_fun(*args):
    print("Callback ", *args)


def increment(number):
    return number + 1

def test_linear(N):
    dfk = DFlowKernel()

    dfk.add_task(0, [], partial(increment, 1))
    for item in range(1, N):
        dfk.add_task(item, [item-1], partial(increment, item))

    print("*"*40, "\nCurrent State , b fulfilled")
    dfk.current_state()

    #dfk.future_resolved(0)

def simple_tests():
    dfk = DFlowKernel()
    dfk.add_task(1, ['a', 'b'], partial(cbk_fun, 1, ['a', 'b']) )
    dfk.current_state()
    dfk.add_task(2, ['a', 'b'], partial(cbk_fun, 2, ['a', 'b', 'v']) )
    print("*"*40, "\nCurrent State")
    dfk.current_state()

    print("*"*40)
    #dfk.future_resolved('a')

    print("*"*40, "\nCurrent State")
    dfk.current_state()
    dfk.check_fulfilled()


    #dfk.future_resolved('b')
    print("*"*40, "\nCurrent State , b fulfilled")
    dfk.current_state()
    dfk.check_fulfilled()


    print("*"*40, "\nCurrent State , END")
    dfk.current_state()

if __name__ == '__main__' :

    test_linear(3)
    #simple_tests()


"""
    def future_resolved(self, fut):
        ''' Implemeting the future_resolved check

        Args:
            fut (Future) : The future to check for
        '''

        if fut not in self.fut_task_lookup:
            print("fut not in fut_task_lookup")
            raise MissingFutError("Missing task:{0} in fut_task_lookup".format(fut))
        else:
            for task_id in self.fut_task_lookup.get(fut, None):
                print("Found : ", task_id)
                print("Dep table : ", self.pending[task_id])
                self.pending[task_id]['dep_cnt'] -= 1

"""
