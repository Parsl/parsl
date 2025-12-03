import os
from pathlib import Path

import pytest

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.dataflow.memoization import BasicMemoizer


def local_config():
    return Config(memoizer=BasicMemoizer(checkpoint_mode="manual"))


@python_app(cache=True)
def uuid_app():
    import uuid
    return uuid.uuid4()


@pytest.mark.local
def test_initial_checkpoint_write() -> None:
    """1. Launch a few apps and write the checkpoint once a few have completed
    """
    uuid_app().result()

    cpt_dir = Path(parsl.dfk().run_dir) / 'checkpoint'

    cptpath = cpt_dir / 'tasks.pkl'

    assert not os.path.exists(cptpath), f"Tasks checkpoint should not exist yet: {cptpath}"
    parsl.dfk().checkpoint()
    assert os.path.exists(cptpath), f"Tasks checkpoint should exist now: {cptpath}"
