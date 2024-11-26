from abc import ABCMeta, abstractproperty


class Channel(metaclass=ABCMeta):
    """Channels are abstractions that enable ExecutionProviders to talk to
    resource managers of remote compute facilities.

    For certain resources such as campus clusters or supercomputers at
    research laboratories, resource requirements may require authentication.
    """

    @abstractproperty
    def script_dir(self) -> str:
        ''' This is a property. Returns the directory assigned for storing all internal scripts such as
        scheduler submit scripts. This is usually where error logs from the scheduler would reside on the
        channel destination side.

        Args:
            - None

        Returns:
            - Channel script dir
        '''
        pass

    # DFK expects to be able to modify this, so it needs to be in the abstract class
    @script_dir.setter
    def script_dir(self, value: str) -> None:
        pass
