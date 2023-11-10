import sys
import radical.pilot as rp

from parsl.serialize import serialize
from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.high_throughput.process_worker_pool import execute_task


class DefaultWorker(rp.raptor.DefaultWorker):

    def _dispatch_func(self, task):

        try:
            buffer = rp.utils.deserialize_bson(task['description']['function'])
            result = execute_task(buffer)
            val = str(serialize(result, buffer_threshold=1000000))
            exc = (None, None)
            ret = 0
            out = None
            err = None
        except Exception as e:
            self._log.debug('Caught an exception: {}'.format(e))
            val = None
            exc = (rp.utils.serialize_bson(RemoteExceptionWrapper(*sys.exc_info())), None)
            ret = 1
            out = None
            err = None

        return out, err, ret, val, exc


class MPIWorker(rp.raptor.MPIWorker):

    def _dispatch_func(self, task):

        return super()._dispatch_func(task)
