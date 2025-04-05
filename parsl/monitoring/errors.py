from parsl.errors import ParslError


class MonitoringRouterStartError(ParslError):
    def __str__(self) -> str:
        return "Monitoring router failed to start"
