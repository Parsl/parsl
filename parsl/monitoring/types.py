from typing import Any, Dict, Tuple, Union
from typing_extensions import TypeAlias
from parsl.monitoring.message_type import MessageType

# A basic parsl monitoring message is wrapped by up to two wrappers:
# The basic monitoring message dictionary can first be tagged, giving
# a TaggedMonitoringMessage, and then that can be further tagged with
# an often unused sender address, giving an AddressedMonitoringMessage.

MonitoringMessage: TypeAlias = Dict[str, Any]
TaggedMonitoringMessage: TypeAlias = Tuple[MessageType, MonitoringMessage]
AddressedMonitoringMessage: TypeAlias = Tuple[TaggedMonitoringMessage, Union[str, int]]
