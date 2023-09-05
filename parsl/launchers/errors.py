from parsl.providers.errors import ExecutionProviderException
from parsl.launchers.base import Launcher


class BadLauncher(ExecutionProviderException, TypeError):
    """Error raised when an object of inappropriate type is supplied as a Launcher
    """

    def __init__(self, launcher: Launcher):
        self.launcher = launcher

    def __str__(self) -> str:
        return f"Bad Launcher provided: {self.launcher}, expecting a parsl.launcher.launcher.Launcher"
