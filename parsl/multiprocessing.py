"""Helpers for cross-plaform multiprocessing support.
"""

import multiprocessing

from typing import Type

ForkProcess: Type = multiprocessing.get_context('fork').Process

