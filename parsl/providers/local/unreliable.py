from parsl.providers.local.local import LocalProvider


class UnreliableLocalProvider(LocalProvider):

    def __init__(self, *args, **kwargs):
        self._unreliable_count = 7
        super().__init__(*args, **kwargs)

    def submit(self, *args, **kwargs):
        if self._unreliable_count > 0:
            self._unreliable_count -= 1
            raise RuntimeError("Unreliable provider still counting down initial failures")

        super().submit(*args, **kwargs)
