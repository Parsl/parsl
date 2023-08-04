from __future__ import annotations

from typing import List

import parsl.jobs.job_status_poller as jsp


class JobErrorHandler:
    def run(self, status: List[jsp.PollItem]):
        for es in status:
            _check_irrecoverable_executor(es)


def _check_irrecoverable_executor(es: jsp.PollItem):
    if not es.executor.error_management_enabled:
        return
    es.executor.handle_errors(es.status)
