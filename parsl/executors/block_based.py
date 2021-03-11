import logging

from abc import abstractmethod, abstractproperty
from parsl.executors.errors import ScalingFailed
from parsl.executors.status_handling import StatusHandlingExecutor
from typing import Any, Dict, List, Tuple, Union

logger = logging.getLogger(__name__)


class BlockProviderExecutor(StatusHandlingExecutor):
    """TODO: basically anything to do with providers/scaling/blocks should be moved into this"""

    def __init__(self, provider):
        super().__init__(provider)
        self.blocks = {}  # type: Dict[str, str]
        self.block_mapping = {}  # type: Dict[str, str]

    def scale_out(self, blocks: int = 1) -> List[str]:
        """Scales out the number of blocks by "blocks"
        """
        if not self.provider:
            raise (ScalingFailed(None, "No execution provider available"))
        block_ids = []
        for i in range(blocks):
            block_id = str(len(self.blocks))
            try:
                job_id = self._launch_block(block_id)
                self.blocks[block_id] = job_id
                self.block_mapping[job_id] = block_id
                block_ids.append(block_id)
            except Exception as ex:
                self._fail_job_async(block_id,
                                     "Failed to start block {}: {}".format(block_id, ex))
        return block_ids

    def _launch_block(self, block_id: str) -> Any:
        launch_cmd = self._get_launch_command(block_id)
        # if self.launch_cmd is None:
        #   raise ScalingFailed(self.provider.label, "No launch command")
        # launch_cmd = self.launch_cmd.format(block_id=block_id)
        job_id = self.provider.submit(launch_cmd, 1)
        logger.debug("Launched block {}->{}".format(block_id, job_id))
        if not job_id:
            raise(ScalingFailed(self.provider.label,
                                "Attempts to provision nodes via provider has failed"))
        return job_id

    @abstractmethod
    def _get_launch_command(self, block_id: str) -> str:
        pass

    def _get_block_and_job_ids(self) -> Tuple[List[str], List[Any]]:
        # Not using self.blocks.keys() and self.blocks.values() simultaneously
        # The dictionary may be changed during invoking this function
        # As scale_in and scale_out are invoked in multiple threads
        block_ids = list(self.blocks.keys())
        job_ids = []  # types: List[Any]
        for bid in block_ids:
            job_ids.append(self.blocks[bid])
        return block_ids, job_ids

    @abstractproperty
    def workers_per_node(self) -> Union[int, float]:
        pass
