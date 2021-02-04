import logging
from abc import abstractmethod
from string import Template

from parsl.providers.error import SchedulerMissingArgs, ScriptPathError
from parsl.launchers.error import BadLauncher
from parsl.providers.provider_base import ExecutionProvider

logger = logging.getLogger(__name__)


class ClusterProvider(ExecutionProvider):
    """ This class defines behavior common to all cluster/supercompute-style scheduler systems.

    Parameters
    ----------
      label : str
        Label for this provider.
      channel : Channel
        Channel for accessing this provider. Possible channels include
        :class:`~parsl.channels.LocalChannel` (the default),
        :class:`~parsl.channels.SSHChannel`, or
        :class:`~parsl.channels.SSHInteractiveLoginChannel`.
      walltime : str
        Walltime requested per block in HH:MM:SS.
      launcher : str
        FIXME
      cmd_timeout : int
        Timeout for commands made to the scheduler in seconds

    .. code:: python

                                +------------------
                                |
          script_string ------->|  submit
               id      <--------|---+
                                |
          [ ids ]       ------->|  status
          [statuses]   <--------|----+
                                |
          [ ids ]       ------->|  cancel
          [cancel]     <--------|----+
                                |
                                +-------------------
    """

    def __init__(self,
                 label,
                 channel,
                 nodes_per_block,
                 init_blocks,
                 min_blocks,
                 max_blocks,
                 parallelism,
                 walltime,
                 launcher,
                 cmd_timeout=10):

        self._label = label
        self.channel = channel
        self.nodes_per_block = nodes_per_block
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.parallelism = parallelism
        self.launcher = launcher
        self.walltime = walltime
        self.cmd_timeout = cmd_timeout
        if not callable(self.launcher):
            raise(BadLauncher(self.launcher,
                              "Launcher for executor: {} is of type: {}. Expects a parsl.launcher.launcher.Launcher or callable".format(
                                  label, type(self.launcher))))

        self.script_dir = None

        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}

    def execute_wait(self, cmd, timeout=None):
        t = self.cmd_timeout
        if timeout is not None:
            t = timeout
        return self.channel.execute_wait(cmd, t)

    def _write_submit_script(self, template, script_filename, job_name, configs):
        """Generate submit script and write it to a file.

        Args:
              - template (string) : The template string to be used for the writing submit script
              - script_filename (string) : Name of the submit script
              - job_name (string) : job name
              - configs (dict) : configs that get pushed into the template

        Returns:
              - True: on success

        Raises:
              SchedulerMissingArgs : If template is missing args
              ScriptPathError : Unable to write submit script out
        """

        try:
            submit_script = Template(template).substitute(jobname=job_name, **configs)
            # submit_script = Template(template).safe_substitute(jobname=job_name, **configs)
            with open(script_filename, 'w') as f:
                f.write(submit_script)

        except KeyError as e:
            logger.error("Missing keys for submit script : %s", e)
            raise (SchedulerMissingArgs(e.args, self.label))

        except IOError as e:
            logger.error("Failed writing to submit script: %s", script_filename)
            raise (ScriptPathError(script_filename, e))
        except Exception as e:
            print("Template : ", template)
            print("Args : ", job_name)
            print("Kwargs : ", configs)
            logger.error("Uncategorized error: %s", e)
            raise (e)

        return True

    @abstractmethod
    def _status(self):
        pass

    def status(self, job_ids):
        """ Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.

        Args:
             - job_ids (list) : A list of job identifiers

        Returns:
             - A list of JobStatus objects corresponding to each job_id in the job_ids list.

        Raises:
             - ExecutionProviderException or its subclasses

        """
        if job_ids:
            self._status()
        return [self.resources[jid]['status'] for jid in job_ids]

    @property
    def label(self):
        return self._label
