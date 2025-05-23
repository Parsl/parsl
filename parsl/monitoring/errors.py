from parsl.errors import ParslError


class MonitoringHubStartError(ParslError):
    def __str__(self) -> str:
        return "Hub failed to start"


class MonitoringRouterStartError(ParslError):
    def __str__(self) -> str:
        return "Monitoring router failed to start"
