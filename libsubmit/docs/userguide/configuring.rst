Configuration
=============

The primary mode by which you interact with libsubmit is by instantiating an ExecutionProvider
with a configuration data structure and optional Channel objects if the ExecutionProvider requires it.

The configuration datastructure expected by an ExecutionProvider as well as options specifics are
described below.

The config structure looks like this:

.. code-block:: python

   config = { "poolName" : <string: Name of the pool>,
              "provider" : <string: Name of provider>,
              "scriptDir" : <string: Path to script directory>,
              "minBlocks" : <int: Minimum number of blocks>,
              "maxBlocks" : <int: Maximum number of blocks>,
              "initBlocks" : <int: Initial number of blocks>,
              "block" : {     # Specify the shape of the block
                  "nodes" : <int: Number of blocs, integer>,
                  "taskBlocks" : <int: Number of task blocks in each block>,
                  "walltime" : <string: walltime in HH:MM:SS format for the block>
                  "options" : { # These are provider specific options
                       "partition" : <string: Name of partition/queue>,
                       "account" : <string: Account id>,
                       "overrides" : <string: String to override and specify options to scheduler>
                  }
            }
