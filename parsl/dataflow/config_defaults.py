"""Home of the configuration defaults."""

import copy
import collections
import pprint
import logging

logger = logging.getLogger(__name__)


def pp_config(config):
    """Pretty print the config.

    This should be part of the default logging to the debug logs.

    Args:
        - config (dict) : The config data structure
    """
    logger.debug(pprint.pformat(config, indent=2))
    return


def recursive_update(template, userdata):
    """Recursively update the template with userdata.

    If we don't do this the value updates for nested collections
    would get simply overwritten rathen than updated.

    Args:
        template (dict) : The templated dict
        userdata (dict) : User supplied config dict structure

    Returns:
        Updated template stucture.
    """
    for k, v in userdata.items():
        if isinstance(v, collections.Mapping):
            template[k] = recursive_update(template.get(k, {}), v)
        else:
            template[k] = v

    return template


def update_config(config, rundir):
    """Update the config datastructure with defaults.

    This is the one centralized location where the default live.

    Args:
         - config (dict) : The config dictionary

    Returns:
         - A standardized config dict if a config was passed, else None.
    """
    if not config:
        return None

    config_base = {"sites": [],
                   "globals": {
                       "lazyErrors": True,   # Bool
                       "usageTracking": True,  # Bool
                       "strategy": 'simple',  # ('simple',...)
                       "appCache": True,  # Bool
                       "checkpoint": None,  # (None, 'periodic', 'dfk_exit', 'task_exit')
                       "checkpointPeriod": None,  # (None, 'HH:MM:SS')
                       "checkpointFiles": None,   # List of checkpoint files
                   },
                   "controller": {
                       "mode": "auto"
                   }
    }

    _config = copy.deepcopy(config)
    sites = copy.deepcopy(_config["sites"])
    del _config["sites"]

    recursive_update(config_base, _config)

    config_sites = []
    for site in sites:
        site_base = {
            "auth": {},
            "execution": {
                "scriptDir": rundir,
                "container": {
                    "type": None,
                    "image": None
                },
                "block": {
                    "nodes": 1,
                    "taskBlocks": 1,
                    "minBlocks": 0,
                    "initBlocks": 0,
                    "maxBlocks": 10,
                    "parallelism": 0.75,
                    "walltime": "00:20:00",
                    "options": {
                    }
                }
            }
        }

        recursive_update(site_base, site)
        config_sites.extend([site_base])

    config_base["sites"] = config_sites

    pp_config(config_base)

    return config_base
