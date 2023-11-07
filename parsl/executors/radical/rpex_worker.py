import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class DefaultWorker(rp.raptor.DefaultWorker):

    def _dispatch_func(self, task):

        out, err, ret, val, exc = super()._dispatch_func(task)
        ser_val = rp.utils.serialize_obj(val)

        return out, err, ret, str(ser_val), exc


# ------------------------------------------------------------------------------
#
class MPIWorker(rp.raptor.MPIWorker):

    def _dispatch_func(self, task):

        return super()._dispatch_func(task)
