from typing import Any, Dict, Tuple
from typing_extensions import TypeAlias
from parsl.monitoring.message_type import MessageType

TaggedMonitoringMessage: TypeAlias = Tuple[MessageType, Dict[str, Any]]
