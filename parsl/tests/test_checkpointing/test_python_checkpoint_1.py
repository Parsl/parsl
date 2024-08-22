import os
from pathlib import Path

import pytest

import parsl
from parsl import python_app
from parsl.tests.configs.local_threads import fresh_config


def local_config():
    config = fresh_config()
    config.checkpoint_mode = "manual"
    return config


@python_app(cache=True)
def uuid_app():
    import uuid
    return uuid.uuid4()


@pytest.mark.local
def test_initial_checkpoint_write() -> None:
    """1. Launch a few apps and write the checkpoint once a few have completed
    """
    uuid_app().result()

    parsl.dfk().checkpoint()

    cpt_dir = Path(parsl.dfk().run_dir) / 'checkpoint'

    cptpath = cpt_dir / 'tasks.pkl'
    assert os.path.exists(cptpath), f"Tasks checkpoint missing: {cptpath}"
