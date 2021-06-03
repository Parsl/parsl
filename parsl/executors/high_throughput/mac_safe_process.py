import multiprocessing


class MacSafeProcess(multiprocessing.get_context('fork').Process):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
