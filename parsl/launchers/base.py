from abc import ABCMeta, abstractmethod

from parsl.utils import RepresentationMixin


class Launcher(RepresentationMixin, metaclass=ABCMeta):
    """Launchers are basically wrappers for user submitted scripts as they
    are submitted to a specific execution resource.
    """
    def __init__(self, debug: bool = True):
        self.debug = debug

    @abstractmethod
    def __call__(self, command: str, tasks_per_node: int, nodes_per_block: int) -> str:
        """ Wraps the command with the Launcher calls.
        """
        pass
