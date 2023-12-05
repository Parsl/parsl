import sys
import radical.pilot as rp

import parsl.app.errors as pe
from parsl.app.bash import remote_side_bash_executor
from parsl.serialize import unpack_apply_message, serialize
from parsl.executors.high_throughput.process_worker_pool import execute_task


class ParslWorker:

    def _dispatch_func(self, task):

        try:
            buffer = rp.utils.deserialize_bson(task['description']['function'])
            result = execute_task(buffer)
            val = str(serialize(result, buffer_threshold=1000000))
            exc = (None, None)
            ret = 0
            out = None
            err = None
        except Exception:
            val = None
            exc = (rp.utils.serialize_bson(pe.RemoteExceptionWrapper(*sys.exc_info())), None)
            ret = 1
            out = None
            err = None

        return out, err, ret, val, exc

    def _dispatch_proc(self, task):

        try:
            buffer = rp.utils.deserialize_bson(task['description']['executable'])
            func, args, kwargs = unpack_apply_message(buffer, {}, copy=False)
            ret = remote_side_bash_executor(func, *args, **kwargs)
            exc = (None, None)
            val = None
            out = None
            err = None
        except Exception:
            val = None
            exc = (rp.utils.serialize_bson(pe.RemoteExceptionWrapper(*sys.exc_info())), None)
            ret = 1
            out = None
            err = None

        return out, err, ret, val, exc


class MPIWorker(rp.raptor.MPIWorker):
    def _dispatch_func(self, task):
        return super()._dispatch_func(task)


class DefaultWorker(rp.raptor.DefaultWorker):
    def _dispatch_func(self, task):
        return ParslWorker()._dispatch_func(task)

    def _dispatch_proc(self, task):
        return ParslWorker()._dispatch_proc(task)
