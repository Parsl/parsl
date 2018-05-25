import atexit
import logging
import os

from libsubmit.launchers import launchers
from libsubmit.providers.provider_base import ExecutionProvider

logger = logging.getLogger(__name__)

try:
    import xmltodict

except ImportError:
    _ge_enabled = False
else:
    _ge_enabled = True

translate_table = {
    'qw': 'PENDING',
    't': 'PENDING',
    'r': 'RUNNING',
    'd': 'COMPLETED',
    'dr': 'STOPPING',
    'rd': 'COMPLETED',  # We shouldn't really see this state
    'c': 'COMPLETED',  # We shouldn't really see this state
}


class GridEngine(ExecutionProvider):
    """ Define the Grid Engine provider

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
          [True/False] <--------|  scaling_enabled
                                |
                                +-------------------
     """

    def __init__(self, config, channel=None):
        ''' Initialize the GridEngine class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs:
             - Channel (None): A channel is required for slurm.
        '''
        self.channel = channel
        self.config = config
        self.sitename = config['site']
        self.provisioned_blocks = 0
        launcher_name = self.config["execution"]["block"].get("launcher", "singleNode")
        self.launcher = launchers.get(launcher_name, None)
        self.script_dir = self.config["execution"]["script_dir"]
        if not os.path.exists(self.script_dir):
            os.makedirs(self.script_dir)
        # Dictionary that keeps track of jobs, keyed on job_id
        self.resources = {}
        atexit.register(self.bye)

    def __repr__(self):
        return "<Grid Engine Execution Provider for site:{0} with channel:{1}>".format(self.sitename, self.channel)

    def create_command(self, path="/local/cluster/bin/:$PATH", lib_path="/local/cluster/lib/"):
        return """qsub -e /dev/null -o /dev/null -terse << EOF
PATH={}
export PATH
LD_LIBRARY_PATH={}
export LD_LIBRARY_PATH
ipengine
EOF
""".format(path, lib_path)

    def submit(self, command="", blocksize=1, job_name="parsl.auto"):
        ''' The submit method takes the command string to be executed upon
        instantiation of a resource most often to start a pilot (such as IPP engine
        or even Swift-T engines).

        Args :
             - command (str) : The bash command string to be executed.
             - blocksize (int) : Blocksize to be requested

        KWargs:
             - job_name (str) : Human friendly name to be assigned to the job request

        Returns:
             - A job identifier, this could be an integer, string etc

        Raises:
             - ExecutionProviderException or its subclasses
        '''
        job_id = None
        try:
            qsub_pilot = """qsub -e /dev/null -o /dev/null -terse << EFO
PATH=/local/cluster/bin/:$PATH
export PATH
LD_LIBRARY_PATH=/local/cluster/lib/
export LD_LIBRARY_PATH
{}
EFO
""".format(command)
            job_id = os.popen(qsub_pilot).read().strip()
            logger.debug("Provisioned a slot")
            new_slot = {
                job_id: {
                    "job_name": job_name,
                    "job_id": job_id,
                    "status": translate_table.get('qw', "PENDING")
                }
            }
            self.resources.update(new_slot)
        except Exception as e:
            logger.error("Failed to provision a slot")
            logger.error(e)
            raise e
        logger.debug("Provisioned {} slots. Started ipengines.")
        self.provisioned_blocks += 1
        return job_id

    def status(self, job_ids):
        ''' Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.

        Args:
             - job_ids (list) : A list of job identifiers

        Returns:
             - A list of status from ['PENDING', 'RUNNING', 'CANCELLED', 'COMPLETED',
               'FAILED', 'TIMEOUT'] corresponding to each job_id in the job_ids list.

        Raises:
             - ExecutionProviderException or its subclasses

        '''

        xml_as_dict = xmltodict.parse(os.popen("qstat -xml").read())
        statuses = []
        j_id = 0
        all_jobs = xml_as_dict.get("job_info", {}).get("queue_info", {})
        all_jobs = all_jobs if all_jobs else {}
        if len(all_jobs.items()) > 1:
            for job_list in all_jobs.get("job_list"):
                status = job_list["state"]
                j_id = job_list["JB_job_number"]
                if j_id in job_ids:
                    statuses.append(translate_table[status])

        elif len(all_jobs.items()) == 1:
            job_list = all_jobs.get("job_list")
            job_list = [job_list] if type(job_list) != list else job_list
            for job in job_list:
                status = job["state"]
                j_id = job["JB_job_number"]
                if j_id in job_ids:
                    statuses.append(translate_table[status])
        else:
            job_list = []
            try:
                job_list = xml_as_dict.get("job_info", {}).get("job_info", {}).get("job_list", [])
                job_list = [job_list] if type(job_list) != list else job_list
            except Exception as e:
                job_list = []
            for job in job_list:
                status = job["state"]
                j_id = job["JB_job_number"]
                if j_id in job_ids:
                    statuses.append(translate_table[status])

        for i in range(len(job_ids) - len(statuses)):
            statuses.append("COMPLETED")
        return statuses

    def cancel(self, job_ids):
        ''' Cancels the resources identified by the job_ids provided by the user.

        Args:
             - job_ids (list): A list of job identifiers

        Returns:
             - A list of status from cancelling the job which can be True, False

        Raises:
             - ExecutionProviderException or its subclasses
        '''
        stati = []
        for job_id in job_ids:
            try:
                outp = os.popen("qdel {}".format(job_id)).read()
            except Exception as e:
                logger.error("failed to cancel job {}".format(job_id))
                outp = "False"
            status = True if "has registered the job" in outp else False
            stati.append(status)
            self.provisioned_blocks -= 1
        return stati

    @property
    def scaling_enabled(self):
        ''' Scaling is enabled

        Returns:
              - Status (Bool)
        '''
        return True

    @property
    def current_capacity(self):
        ''' Returns the number of currently provisioned blocks.
        This may need to return more information in the futures :
        { minsize, maxsize, current_requested }
        '''
        return self.provisioned_blocks

    @property
    def channels_required(self):
        ''' GridEngine does not require a channel

        Returns:
              - Status (Bool)
        '''
        return False

    def bye(self):
        self.cancel([i for i in list(self.resources)])
