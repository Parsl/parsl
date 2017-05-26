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
import logging
from inspect import signature
from concurrent.futures import Future
from functools import partial

from parsl.dataflow.error import *
from parsl.dataflow.states import States
from parsl.dataflow.futures import AppFuture

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

        #self.pending         = {}
        #self.runnable        = {}
        #self.done            = {}

        self.fut_task_lookup = {}
        self.tasks           = {}
        self.executor        = executor

    @staticmethod
    def _count_deps(depends, task_id):
        ''' Internal. Count the number of unresolved futures in the list depends'''

        count = 0
        for dep in depends:
            if isinstance(dep, Future) or issubclass(type(dep), Future):
                logger.debug("Task:%s dep:%s done:%s", task_id, dep, dep.done())
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
            self.tasks[task_id]['status'] = States.done
        else:
            logger.debug("Failed    : %s with %s", task_id, future)
            self.tasks[task_id]['status'] = States.failed

        # Identify tasks that have resolved dependencies and launch
        for tid in list(self.tasks):
            # Skip all non-pending tasks
            if self.tasks[tid]['status'] != States.pending:
                continue

            if self._count_deps(self.tasks[tid]['depends'], tid) == 0:
                # We can now launch *task*
                logger.debug("Task : %s is now runnable", tid)
                new_args, kwargs = self.sanitize_and_wrap(task_id,
                                                          self.tasks[tid]['args'],
                                                          self.tasks[tid]['kwargs'])
                self.tasks[tid]['status'] = States.running
                exec_fu = self.launch_task(task_id, self.tasks[tid]['func'], *new_args, **kwargs)
                self.tasks[task_id]['exec_fu'] = exec_fu

                try:
                    self.tasks[tid]['app_fu'].update_parent(exec_fu)
                    self.tasks[tid]['exec_fu'] = exec_fu
                except AttributeError as e:
                    logger.error("Caught AttributeError at update_parent for task:%s", tid)
                    raise e

        return


    def write_status_log(self):
        ''' Write status log.

        Args:
           None

        Kwargs:
           None
        '''

        state_lens = {States.unsched : 0,
                      States.pending : 0,
                      States.runnable: 0,
                      States.running : 0,
                      States.done    : 0,
                      States.failed  : 0,
                      States.dep_fail: 0}

        for tid in self.tasks:
            state_lens[self.tasks[tid]['status']] += 1

        logger.debug("Pending:%d   Runnable:%d   Done:%d", state_lens[States.pending],
                     state_lens[States.runnable],
                     state_lens[States.done])


    def print_status_log(self):
        ''' Print status log in terms of pending, runnable and done tasks

        Args:
           None

        Kwargs:
           None
        '''

        state_lens = {States.unsched : 0,
                      States.pending : 0,
                      States.runnable: 0,
                      States.running : 0,
                      States.done    : 0,
                      States.failed  : 0,
                      States.dep_fail: 0}

        for tid in self.tasks:
            state_lens[self.tasks[tid]['status']] += 1

        print("Pending:{0}   Runnable:{1}   Done:{2}".format( state_lens[States.pending],
                                                              state_lens[States.runnable],
                                                              state_lens[States.done] ))

    def launch_task(self, task_id, executable, *args, **kwargs):
        ''' Handle the actual submission of the task to the executor layer

        We should most likely add a callback at this point

        Args:
            task_id (uuid string) : A uuid string that uniquely identifies the task
            executable (callable) : A callable object
            args (list of positional args)
            kwargs (list of keyword args)


        Returns:
            Future that tracks the execution of the submitted executable
        '''

        logger.debug("Submitting to executor : %s", task_id)
        exec_fu = self.executor.submit(executable, *args, **kwargs)
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
    def sanitize_and_wrap(task_id, args, kwargs):
        ''' This function should be called **ONLY** when all the futures we track
        have been resolved. If the user hid futures a level below, we will not catch
        it, and will (most likely) result in a type error .

        Args:
             task_id (uuid str) : Task id
             func (Function) : App function
             args (List) : Positional args to app function
             kwargs (Dict) : Kwargs to app function

        Return:
             partial Function evaluated with all dependencies in  args, kwargs and kwargs['inputs'] evaluated.

        '''

        # Todo: This function is not tested.
        logger.debug("Task:%s Sanitizing %s %s", task_id, args, kwargs)

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

        return new_args, kwargs


    def submit (self, func, *args, **kwargs):
        ''' Add task to the dataflow system.

        If all deps are met :
              send to the runnable queue
              and launch the task
        Else:
              post the task in the pending queue

        Returns:
               (AppFuture) [DataFutures,]
        '''

        task_id  = uuid.uuid4()
        dep_cnt, depends = self._count_all_deps(task_id, args, kwargs)

        #dep_cnt  = self._count_deps(depends, task_id)
        task_def = { 'depends'    : depends,
                     'func'       : func,
                     'args'       : args,
                     'kwargs'     : kwargs,
                     'callback'   : None,
                     'dep_cnt'    : dep_cnt,
                     'exec_fu'    : None,
                     'status'     : States.unsched,
                     'app_fu'     : None  }

        if task_id in self.tasks:
            raise DuplicateTaskError("Task {0} in pending list".format(task_id))
        else:
            self.tasks[task_id] = task_def

        if dep_cnt == 0 :
            # Set to running
            logger.debug("Task:%s setting to running", task_id)
            new_args, kwargs = self.sanitize_and_wrap(task_id, args, kwargs)
            self.tasks[task_id]['exec_fu'] = self.launch_task(task_id, func, *new_args, **kwargs)
            self.tasks[task_id]['app_fu']  = AppFuture(self.tasks[task_id]['exec_fu'])
            self.tasks[task_id]['status']  = States.running
            logger.debug("Task : %s ", self.tasks[task_id])
        else:
            # Send to pending
            logger.debug("Task:%s setting to pending", task_id)
            self.tasks[task_id]['app_fu']  = AppFuture(None)
            self.tasks[task_id]['status']  = States.pending

        logger.debug("Task:%s Launched with AppFut:%s", task_id, task_def['app_fu'])
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
