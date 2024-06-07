"""Module for defining the usage tracking levels."""

DISABLED = 0  # Tracking is disabled
LEVEL_1 = 1  # Share info about Parsl version, Python version, platform
LEVEL_2 = 2  # Share info about config + level 1
LEVEL_3 = 3  # Share info about app count, app fails, execution time + level 2
