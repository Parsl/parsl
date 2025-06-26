from parsl.errors import ParslError


class MonitoringRouterStartError(ParslError):
    def __str__(self) -> str:
        return "Monitoring router failed to start"


class RadioRequiredError(ParslError):
    def __str__(self) -> str:
        return "A radio must be configured for remote task monitoring"
