from typing import Any, Dict, Tuple

from typing_extensions import TypeAlias

from parsl.monitoring.message_type import MessageType

# A MonitoringMessage dictionary can be tagged, giving a
# TaggedMonitoringMessage.

MonitoringMessage: TypeAlias = Dict[str, Any]
TaggedMonitoringMessage: TypeAlias = Tuple[MessageType, MonitoringMessage]
