import json
import logging
import os
import time
from string import Template

from parsl.dataflow.error import ConfigurationError
from parsl.providers.aws.template import template_string
from parsl.providers.provider_base import ExecutionProvider, JobState, JobStatus
from parsl.providers.error import OptionalModuleMissing
from parsl.utils import RepresentationMixin
from parsl.launchers import SingleNodeLauncher

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError

except ImportError:
    _boto_enabled = False
else:
    _boto_enabled = True

translate_table = {
    'pending': JobState.PENDING,
    'running': JobState.RUNNING,
    'terminated': JobState.COMPLETED,
    'shutting-down': JobState.COMPLETED,  # (configuring),
    'stopping': JobState.COMPLETED,  # We shouldn't really see this state
    'stopped': JobState.COMPLETED,  # We shouldn't really see this state
}


class AWSProvider(ExecutionProvider, RepresentationMixin):
    """A provider for using Amazon Elastic Compute Cloud (EC2) resources.

    One of 3 methods are required to authenticate: keyfile, profile or environment
    variables. If neither keyfile or profile are set, the following environment
    variables must be set: `AWS_ACCESS_KEY_ID` (the access key for your AWS account),
    `AWS_SECRET_ACCESS_KEY` (the secret key for your AWS account), and (optionaly) the
    `AWS_SESSION_TOKEN` (the session key for your AWS account).

    Parameters
    ----------
    image_id : str
        Identification of the Amazon Machine Image (AMI).
    worker_init : str
        String to append to the Userdata script executed in the cloudinit phase of
        instance initialization.
    walltime : str
        Walltime requested per block in HH:MM:SS.
    key_file : str
        Path to json file that contains 'AWSAccessKeyId' and 'AWSSecretKey'.
    nodes_per_block : int
        This is always 1 for ec2. Nodes to provision per block.
    profile : str
        Profile to be used from the standard aws config file ~/.aws/config.
    nodes_per_block : int
        Nodes to provision per block. Default is 1.
    init_blocks : int
        Number of blocks to provision at the start of the run. Default is 1.
    min_blocks : int
        Minimum number of blocks to maintain. Default is 0.
    max_blocks : int
        Maximum number of blocks to maintain. Default is 10.
    instance_type : str
        EC2 instance type. Instance types comprise varying combinations of CPU, memory,  .
        storage, and networking capacity For more information on possible instance types,.
        see `here <https://aws.amazon.com/ec2/instance-types/>`_ Default is 't2.small'.
    region : str
        Amazon Web Service (AWS) region to launch machines. Default is 'us-east-2'.
    key_name : str
        Name of the AWS private key (.pem file) that is usually generated on the console
        to allow SSH access to the EC2 instances. This is mostly used for debugging.
    spot_max_bid : float
        Maximum bid price (if requesting spot market machines).
    iam_instance_profile_arn : str
        Launch instance with a specific role.
    state_file : str
        Path to the state file from a previous run to re-use.
    walltime : str
        Walltime requested per block in HH:MM:SS. This option is not currently honored by this provider.
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~parsl.launchers.SingleNodeLauncher` (the default),
        :class:`~parsl.launchers.SrunLauncher`, or
        :class:`~parsl.launchers.AprunLauncher`
    linger : Bool
        When set to True, the workers will not `halt`. The user is responsible for shutting
        down the node.
    """

    def __init__(self,
                 image_id,
                 key_name,
                 init_blocks=1,
                 min_blocks=0,
                 max_blocks=10,
                 nodes_per_block=1,
                 parallelism=1,

                 worker_init='',
                 instance_type='t2.small',
                 region='us-east-2',
                 spot_max_bid=0,

                 key_file=None,
                 profile=None,
                 iam_instance_profile_arn='',

                 state_file=None,
                 walltime="01:00:00",
                 linger=False,
                 launcher=SingleNodeLauncher()):
        if not _boto_enabled:
            raise OptionalModuleMissing(['boto3'], "AWS Provider requires the boto3 module.")

        self.image_id = image_id
        self._label = 'ec2'
        self.init_blocks = init_blocks
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.nodes_per_block = nodes_per_block
        self.max_nodes = max_blocks * nodes_per_block
        self.parallelism = parallelism

        self.worker_init = worker_init
        self.instance_type = instance_type
        self.region = region
        self.spot_max_bid = spot_max_bid

        self.key_name = key_name
        self.key_file = key_file
        self.profile = profile
        self.iam_instance_profile_arn = iam_instance_profile_arn

        self.walltime = walltime
        self.launcher = launcher
        self.linger = linger
        self.resources = {}
        self.state_file = state_file if state_file is not None else '.ec2_{}.json'.format(self.label)

        env_specified = os.getenv("AWS_ACCESS_KEY_ID") is not None and os.getenv("AWS_SECRET_ACCESS_KEY") is not None
        if profile is None and key_file is None and not env_specified:
            raise ConfigurationError("Must specify either profile', 'key_file', or "
                                     "'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' environment variables.")

        try:
            self.initialize_boto_client()
        except Exception as e:
            logger.error("{} failed to initialize.".format(self))
            raise e

        state_file_exists = False
        try:
            self.read_state_file(self.state_file)
            state_file_exists = True
        except Exception:
            logger.info("No state file found. Cannot load previous options. Creating new infrastructure.")

        if not state_file_exists:
            try:
                self.create_vpc().id
            except Exception as e:
                logger.info("Failed to create ec2 infrastructure: {0}".format(e))
                raise
            else:
                self.write_state_file()

    def initialize_boto_client(self):
        """Initialize the boto client."""

        self.session = self.create_session()
        self.client = self.session.client('ec2')
        self.ec2 = self.session.resource('ec2')
        self.instances = []
        self.instance_states = {}
        self.vpc_id = 0
        self.sg_id = 0
        self.sn_ids = []

    def read_state_file(self, state_file):
        """Read the state file, if it exists.

        If this script has been run previously, resource IDs will have been written to a
        state file. On starting a run, a state file will be looked for before creating new
        infrastructure. Information on VPCs, security groups, and subnets are saved, as
        well as running instances and their states.

        AWS has a maximum number of VPCs per region per account, so we do not want to
        clutter users' AWS accounts with security groups and VPCs that will be used only
        once.
        """
        try:
            fh = open(state_file, 'r')
            state = json.load(fh)
            self.vpc_id = state['vpcID']
            self.sg_id = state['sgID']
            self.sn_ids = state['snIDs']
            self.instances = state['instances']
        except Exception as e:
            logger.debug("Caught exception while reading state file: {0}".format(e))
            raise e
        logger.debug("Done reading state from the local state file.")

    def write_state_file(self):
        """Save information that must persist to a file.

        We do not want to create a new VPC and new identical security groups, so we save
        information about them in a file between runs.
        """
        fh = open('awsproviderstate.json', 'w')
        state = {}
        state['vpcID'] = self.vpc_id
        state['sgID'] = self.sg_id
        state['snIDs'] = self.sn_ids
        state['instances'] = self.instances
        state["instanceState"] = self.instance_states
        fh.write(json.dumps(state, indent=4))

    def create_session(self):
        """Create a session.

        First we look in self.key_file for a path to a json file with the
        credentials. The key file should have 'AWSAccessKeyId' and 'AWSSecretKey'.

        Next we look at self.profile for a profile name and try
        to use the Session call to automatically pick up the keys for the profile from
        the user default keys file ~/.aws/config.

        Finally, boto3 will look for the keys in environment variables:
        AWS_ACCESS_KEY_ID: The access key for your AWS account.
        AWS_SECRET_ACCESS_KEY: The secret key for your AWS account.
        AWS_SESSION_TOKEN: The session key for your AWS account.
        This is only needed when you are using temporary credentials.
        The AWS_SECURITY_TOKEN environment variable can also be used,
        but is only supported for backwards compatibility purposes.
        AWS_SESSION_TOKEN is supported by multiple AWS SDKs besides python.
        """

        session = None

        if self.key_file is not None:
            credfile = os.path.expandvars(os.path.expanduser(self.key_file))

            try:
                with open(credfile, 'r') as f:
                    creds = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(
                    "EC2Provider '{}': json decode error in credential file {}".format(self.label, credfile)
                )
                raise e

            except Exception as e:
                logger.debug(
                    "EC2Provider '{0}' caught exception while reading credential file: {1}".format(
                        self.label, credfile
                    )
                )
                raise e

            logger.debug("EC2Provider '{}': Using credential file to create session".format(self.label))
            session = boto3.session.Session(region_name=self.region, **creds)
        elif self.profile is not None:
            logger.debug("EC2Provider '{}': Using profile name to create session".format(self.label))
            session = boto3.session.Session(
                profile_name=self.profile, region_name=self.region
            )
        else:
            logger.debug("EC2Provider '{}': Using environment variables to create session".format(self.label))
            session = boto3.session.Session(region_name=self.region)

        return session

    def create_vpc(self):
        """Create and configure VPC

        We create a VPC with CIDR 10.0.0.0/16, which provides up to 64,000 instances.

        We attach a subnet for each availability zone within the region specified in the
        config. We give each subnet an ip range like 10.0.X.0/20, which is large enough
        for approx. 4000 instances.

        Security groups are configured in function security_group.
        """

        try:
            # We use a large VPC so that the cluster can get large
            vpc = self.ec2.create_vpc(
                CidrBlock='10.0.0.0/16',
                AmazonProvidedIpv6CidrBlock=False,
            )
        except Exception as e:
            # This failure will cause a full abort
            logger.error("{}\n".format(e))
            raise e

        # Attach internet gateway so that our cluster can
        # talk to the outside internet
        internet_gateway = self.ec2.create_internet_gateway()
        internet_gateway.attach_to_vpc(VpcId=vpc.vpc_id)  # Returns None
        self.internet_gateway = internet_gateway.id

        # Create and configure route table to allow proper traffic
        route_table = self.config_route_table(vpc, internet_gateway)
        self.route_table = route_table.id

        # Get all avaliability zones
        availability_zones = self.client.describe_availability_zones()

        # go through AZs and set up a subnet per
        for num, zone in enumerate(availability_zones['AvailabilityZones']):
            if zone['State'] == "available":

                # Create a large subnet (4000 max nodes)
                subnet = vpc.create_subnet(
                    CidrBlock='10.0.{}.0/20'.format(16 * num), AvailabilityZone=zone['ZoneName']
                )

                # Make subnet accessible
                subnet.meta.client.modify_subnet_attribute(
                    SubnetId=subnet.id, MapPublicIpOnLaunch={"Value": True}
                )

                route_table.associate_with_subnet(SubnetId=subnet.id)
                self.sn_ids.append(subnet.id)
            else:
                logger.info("{} unavailable".format(zone['ZoneName']))
        # Security groups
        self.security_group(vpc)
        self.vpc_id = vpc.id
        return vpc

    def security_group(self, vpc):
        """Create and configure a new security group.

        Allows all ICMP in, all TCP and UDP in within VPC.

        This security group is very open. It allows all incoming ping requests on all
        ports. It also allows all outgoing traffic on all ports. This can be limited by
        changing the allowed port ranges.

        Parameters
        ----------
        vpc : VPC instance
            VPC in which to set up security group.
        """

        sg = vpc.create_security_group(
            GroupName="private-subnet", Description="security group for remote executors"
        )

        ip_ranges = [{'CidrIp': '10.0.0.0/16'}]

        # Allows all ICMP in, all TCP and UDP in within VPC
        in_permissions = [
            {
                'IpProtocol': 'TCP',
                'FromPort': 0,
                'ToPort': 65535,
                'IpRanges': ip_ranges,
            }, {
                'IpProtocol': 'UDP',
                'FromPort': 0,
                'ToPort': 65535,
                'IpRanges': ip_ranges,
            }, {
                'IpProtocol': 'ICMP',
                'FromPort': -1,
                'ToPort': -1,
                'IpRanges': [{
                    'CidrIp': '0.0.0.0/0'
                }],
            }, {
                'IpProtocol': 'TCP',
                'FromPort': 22,
                'ToPort': 22,
                'IpRanges': [{
                    'CidrIp': '0.0.0.0/0'
                }],
            }
        ]

        # Allows all TCP out, all TCP and UDP out within VPC
        out_permissions = [
            {
                'IpProtocol': 'TCP',
                'FromPort': 0,
                'ToPort': 65535,
                'IpRanges': [{
                    'CidrIp': '0.0.0.0/0'
                }],
            },
            {
                'IpProtocol': 'TCP',
                'FromPort': 0,
                'ToPort': 65535,
                'IpRanges': ip_ranges,
            },
            {
                'IpProtocol': 'UDP',
                'FromPort': 0,
                'ToPort': 65535,
                'IpRanges': ip_ranges,
            },
        ]

        sg.authorize_ingress(IpPermissions=in_permissions)
        sg.authorize_egress(IpPermissions=out_permissions)
        self.sg_id = sg.id
        return sg

    def config_route_table(self, vpc, internet_gateway):
        """Configure route table for Virtual Private Cloud (VPC).

        Parameters
        ----------
        vpc : dict
            Representation of the VPC (created by create_vpc()).
        internet_gateway : dict
            Representation of the internet gateway (created by create_vpc()).
        """
        route_table = vpc.create_route_table()
        route_table.create_route(
            DestinationCidrBlock='0.0.0.0/0', GatewayId=internet_gateway.internet_gateway_id
        )
        return route_table

    def xstr(self, s):
        return '' if s is None else s

    def spin_up_instance(self, command, job_name):
        """Start an instance in the VPC in the first available subnet.

        N instances will be started if nodes_per_block > 1.
        Not supported. We only do 1 node per block.

        Parameters
        ----------
        command : str
            Command string to execute on the node.
        job_name : str
            Name associated with the instances.
        """

        command = Template(template_string).substitute(jobname=job_name,
                                                       user_script=command,
                                                       linger=str(self.linger).lower(),
                                                       worker_init=self.worker_init)
        instance_type = self.instance_type
        subnet = self.sn_ids[0]
        ami_id = self.image_id
        total_instances = len(self.instances)

        if float(self.spot_max_bid) > 0:
            spot_options = {
                'MarketType': 'spot',
                'SpotOptions': {
                    'MaxPrice': str(self.spot_max_bid),
                    'SpotInstanceType': 'one-time',
                    'InstanceInterruptionBehavior': 'terminate'
                }
            }
        else:
            spot_options = {}

        if total_instances > self.max_nodes:
            logger.warning("Exceeded instance limit ({}). Cannot continue\n".format(self.max_nodes))
            return [None]
        try:
            tag_spec = [{"ResourceType": "instance", "Tags": [{'Key': 'Name', 'Value': job_name}]}]

            instance = self.ec2.create_instances(
                MinCount=1,
                MaxCount=1,
                InstanceType=instance_type,
                ImageId=ami_id,
                KeyName=self.key_name,
                SubnetId=subnet,
                SecurityGroupIds=[self.sg_id],
                TagSpecifications=tag_spec,
                InstanceMarketOptions=spot_options,
                InstanceInitiatedShutdownBehavior='terminate',
                IamInstanceProfile={'Arn': self.iam_instance_profile_arn},
                UserData=command
            )
        except ClientError as e:
            print(e)
            logger.error(e.response)
            return [None]

        except Exception as e:
            logger.error("Request for EC2 resources failed : {0}".format(e))
            return [None]

        self.instances.append(instance[0].id)
        logger.info(
            "Started up 1 instance {}. Instance type: {}".format(instance[0].id, instance_type)
        )
        return instance

    def shut_down_instance(self, instances=None):
        """Shut down a list of instances, if provided.

        If no instance is provided, the last instance started up will be shut down.
        """

        if instances and len(self.instances) > 0:
            print(instances)
            try:
                print([i.id for i in instances])
            except Exception as e:
                print(e)
            term = self.client.terminate_instances(InstanceIds=instances)
            logger.info("Shut down {} instances (ids:{}".format(len(instances), str(instances)))
        elif len(self.instances) > 0:
            instance = self.instances.pop()
            term = self.client.terminate_instances(InstanceIds=[instance])
            logger.info("Shut down 1 instance (id:{})".format(instance))
        else:
            logger.warning("No Instances to shut down.\n")
            return -1
        self.get_instance_state()
        return term

    def get_instance_state(self, instances=None):
        """Get states of all instances on EC2 which were started by this file."""
        if instances:
            desc = self.client.describe_instances(InstanceIds=instances)
        else:
            desc = self.client.describe_instances(InstanceIds=self.instances)
        # pprint.pprint(desc['Reservations'],indent=4)
        for i in range(len(desc['Reservations'])):
            instance = desc['Reservations'][i]['Instances'][0]
            self.instance_states[instance['InstanceId']] = instance['State']['Name']
        return self.instance_states

    def status(self, job_ids):
        """Get the status of a list of jobs identified by their ids.

        Parameters
        ----------
        job_ids : list of str
            Identifiers for the jobs.

        Returns
        -------
        list of int
            The status codes of the requsted jobs.
        """

        all_states = []

        status = self.client.describe_instances(InstanceIds=list(job_ids))
        for r in status['Reservations']:
            for i in r['Instances']:
                instance_id = i['InstanceId']
                instance_state = translate_table.get(i['State']['Name'], JobState.UNKNOWN)
                self.resources[instance_id]['status'] = JobStatus(instance_state)
                all_states.extend([instance_state])

        return all_states

    def submit(self, command='sleep 1', tasks_per_node=1, job_name="parsl.aws"):
        """Submit the command onto a freshly instantiated AWS EC2 instance.

        Submit returns an ID that corresponds to the task that was just submitted.

        Parameters
        ----------
        command : str
            Command to be invoked on the remote side.
        tasks_per_node : int (default=1)
            Number of command invocations to be launched per node
        job_name : str
            Prefix for the job name.

        Returns
        -------
        None or str
            If at capacity, None will be returned. Otherwise, the job identifier will be returned.
        """

        job_name = "parsl.aws.{0}".format(time.time())
        wrapped_cmd = self.launcher(command,
                                    tasks_per_node,
                                    self.nodes_per_block)
        [instance, *rest] = self.spin_up_instance(command=wrapped_cmd, job_name=job_name)

        if not instance:
            logger.error("Failed to submit request to EC2")
            return None

        logger.debug("Started instance_id: {0}".format(instance.instance_id))

        state = translate_table.get(instance.state['Name'], JobState.PENDING)

        self.resources[instance.instance_id] = {
            "job_id": instance.instance_id,
            "instance": instance,
            "status": JobStatus(state)
        }

        return instance.instance_id

    def cancel(self, job_ids):
        """Cancel the jobs specified by a list of job ids.

        Parameters
        ----------
        job_ids : list of str
            List of of job identifiers

        Returns
        -------
        list of bool
            Each entry in the list will contain False if the operation fails. Otherwise, the entry will be True.
        """

        if self.linger is True:
            logger.debug("Ignoring cancel requests due to linger mode")
            return [False for x in job_ids]

        try:
            self.client.terminate_instances(InstanceIds=list(job_ids))
        except Exception as e:
            logger.error("Caught error while attempting to remove instances: {0}".format(job_ids))
            raise e
        else:
            logger.debug("Removed the instances: {0}".format(job_ids))

        for job_id in job_ids:
            self.resources[job_id]["status"] = JobStatus(JobState.COMPLETED)

        for job_id in job_ids:
            self.instances.remove(job_id)

        return [True for x in job_ids]

    def show_summary(self):
        """Print human readable summary of current AWS state to log and to console."""
        self.get_instance_state()
        status_string = "EC2 Summary:\n\tVPC IDs: {}\n\tSubnet IDs: \
{}\n\tSecurity Group ID: {}\n\tRunning Instance IDs: {}\n".format(
            self.vpc_id, self.sn_ids, self.sg_id, self.instances
        )
        status_string += "\tInstance States:\n\t\t"
        self.get_instance_state()
        for state in self.instance_states.keys():
            status_string += "Instance ID: {}  State: {}\n\t\t".format(
                state, self.instance_states[state]
            )
        status_string += "\n"
        logger.info(status_string)
        return status_string

    def teardown(self):
        """Teardown the EC2 infastructure.

        Terminate all EC2 instances, delete all subnets, delete security group, delete VPC,
        and reset all instance variables.
        """

        self.shut_down_instance(self.instances)
        self.instances = []
        try:
            self.client.delete_internet_gateway(InternetGatewayId=self.internet_gateway)
            self.internet_gateway = None
            self.client.delete_route_table(RouteTableId=self.route_table)
            self.route_table = None
            for subnet in list(self.sn_ids):
                # Cast to list ensures that this is a copy
                # Which is important because it means that
                # the length of the list won't change during iteration
                self.client.delete_subnet(SubnetId=subnet)
                self.sn_ids.remove(subnet)
            self.client.delete_security_group(GroupId=self.sg_id)
            self.sg_id = None
            self.client.delete_vpc(VpcId=self.vpc_id)
            self.vpc_id = None
        except Exception as e:
            logger.error("{}".format(e))
            raise e
        self.show_summary()
        os.remove(self.config['state_file_path'])

    @property
    def label(self):
        return self._label

    @property
    def current_capacity(self):
        """Returns the current blocksize."""
        return len(self.instances)

    def goodbye(self):
        self.teardown()

    @property
    def status_polling_interval(self):
        return 60
