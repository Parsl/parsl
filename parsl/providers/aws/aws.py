import json
import logging
import os
import time
from string import Template

from parsl.errors import ConfigurationError, OptionalModuleMissing
from parsl.jobs.states import JobState, JobStatus
from parsl.launchers import SingleNodeLauncher
from parsl.providers.aws.template import template_string
from parsl.providers.base import ExecutionProvider
from parsl.utils import RepresentationMixin

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

# Tags applied to every EC2 resource created by AWSProvider. teardown() only
# deletes resources tagged with the infrastructure ID recorded in the state file.
PROVIDER_TAG_KEY = 'parsl:provider'
INFRASTRUCTURE_TAG_KEY = 'parsl:infrastructure'

# resource kind -> (describe method, ID list parameter, result key)
_DESCRIBE_RESOURCE = {
    'VPC': ('describe_vpcs', 'VpcIds', 'Vpcs'),
    'security group': ('describe_security_groups', 'GroupIds', 'SecurityGroups'),
    'internet gateway': ('describe_internet_gateways', 'InternetGatewayIds', 'InternetGateways'),
    'subnet': ('describe_subnets', 'SubnetIds', 'Subnets'),
    'route table': ('describe_route_tables', 'RouteTableIds', 'RouteTables'),
}


class AWSProvider(ExecutionProvider, RepresentationMixin):
    """A provider for using Amazon Elastic Compute Cloud (EC2) resources.

    One of 3 methods are required to authenticate: keyfile, profile or environment
    variables. If neither keyfile or profile are set, the following environment
    variables must be set: ``AWS_ACCESS_KEY_ID`` (the access key for your AWS account),
    ``AWS_SECRET_ACCESS_KEY`` (the secret key for your AWS account), and (optionaly) the
    ``AWS_SESSION_TOKEN`` (the session key for your AWS account).

    Every EC2 resource created by this provider (VPC, subnets, security group,
    internet gateway, route table and instances) is tagged with
    ``parsl:provider=AWSProvider`` and ``parsl:infrastructure=<infrastructure id>``,
    where the infrastructure ID is recorded in the state file. :meth:`teardown`
    only deletes resources carrying the matching ``parsl:infrastructure`` tag.

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
        EC2 instance type. Instance types comprise varying combinations of CPU, memory,
        storage, and networking capacity For more information on possible instance types,
        see `here <https://aws.amazon.com/ec2/instance-types/>`_. Default is 't2.small'.
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
        Path to the state file recording the EC2 infrastructure (VPC, subnets,
        security group, internet gateway and route table) created by this provider.
        If the file exists, that infrastructure is re-used; otherwise new
        infrastructure is created and recorded there. A relative path, including the
        default ``awsproviderstate.json``, is relative to the current working directory.
        The file is managed by the provider and should not be written by hand:
        :meth:`teardown` will not delete resources which do not carry the
        infrastructure ID it records, including infrastructure recorded by older
        versions of Parsl, which did not tag resources.
    walltime : str
        Walltime requested per block in HH:MM:SS. This option is not currently honored by this provider.
    launcher : Launcher
        Launcher for this provider. With AWS, usually the default
        :class:`~parsl.launchers.SingleNodeLauncher` will be appropriate.
    linger : Bool
        When set to True, the workers will not ``halt``. The user is responsible for shutting
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
        self.script_dir = None
        self.state_file = state_file if state_file is not None else 'awsproviderstate.json'

        env_specified = os.getenv("AWS_ACCESS_KEY_ID") is not None and os.getenv("AWS_SECRET_ACCESS_KEY") is not None
        if profile is None and key_file is None and not env_specified:
            raise ConfigurationError("Must specify either profile', 'key_file', or "
                                     "'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' environment variables.")

        try:
            self.initialize_boto_client()
        except Exception as e:
            logger.error("{} failed to initialize.".format(self))
            raise e

        state_file_path = os.path.abspath(self.state_file)
        try:
            self.read_state_file(self.state_file)
        except FileNotFoundError:
            logger.info("No state file found at {}. Creating new EC2 infrastructure.".format(state_file_path))
            self.create_infrastructure()
            logger.info("Recorded new EC2 infrastructure in state file {}".format(state_file_path))
        else:
            logger.info("Re-using EC2 infrastructure from state file {}".format(state_file_path))

    def create_infrastructure(self):
        """Create the VPC and related resources, and record them in the state file.

        If creation fails part way through, the resources created so far are removed
        (best-effort) with :meth:`teardown` before the exception is re-raised, so that
        a failed start does not leave orphaned resources behind with no state file
        recording them.
        """
        try:
            self.create_vpc()
            self.write_state_file()
        except BaseException as e:
            logger.error("Failed to create EC2 infrastructure: {}. "
                         "Removing partially created resources.".format(e))
            try:
                if not self.teardown():
                    logger.error("Some partially created EC2 infrastructure was not removed; "
                                 "see the warnings above")
            except Exception:
                logger.exception("Failed to remove partially created EC2 infrastructure; "
                                 "these resources must be removed manually: VPC {}, internet gateway {}, "
                                 "route table {}, subnets {}, security group {}".format(
                                     self.vpc_id, self.internet_gateway, self.route_table,
                                     self.sn_ids, self.sg_id))
            raise

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
        self.internet_gateway = None
        self.route_table = None
        self.infrastructure_id = None

    def read_state_file(self, state_file):
        """Read the state file, if it exists.

        If this script has been run previously, resource IDs will have been written to a
        state file. On starting a run, a state file will be looked for before creating new
        infrastructure. Information on VPCs, security groups, subnets, the internet gateway
        and the route table is saved.

        Instance IDs from a previous run are not restored: they belong to a previous
        session and are not tracked by this one. Any instances still running in the VPC
        are discovered and terminated by :meth:`teardown`.

        AWS has a maximum number of VPCs per region per account, so we do not want to
        clutter users' AWS accounts with security groups and VPCs that will be used only
        once.

        Raises
        ------
        FileNotFoundError
            If the state file does not exist.
        ConfigurationError
            If the state file exists but cannot be read or does not describe a VPC.
        """
        try:
            with open(state_file, 'r') as fh:
                state = json.load(fh)
            vpc_id = state['vpcID']
            sg_id = state['sgID']
            sn_ids = state['snIDs']
            # Not present in state files written by older versions of this provider,
            # which did not tag their resources; teardown() leaves those in place.
            internet_gateway = state.get('igwID')
            route_table = state.get('rtbID')
            infrastructure_id = state.get('infrastructureID')
            if not (isinstance(vpc_id, str) and vpc_id and isinstance(sg_id, str) and sg_id):
                raise ValueError("vpcID and sgID must be non-empty strings")
            if not (isinstance(sn_ids, list) and all(isinstance(sn, str) for sn in sn_ids)):
                raise ValueError("snIDs must be a list of strings")
            if not (infrastructure_id is None or isinstance(infrastructure_id, str)):
                raise ValueError("infrastructureID must be a string")
        except FileNotFoundError:
            raise
        except Exception as e:
            raise ConfigurationError(
                "Could not load AWS provider state file {}: {!r}. Fix or remove the file; "
                "if it is removed, any EC2 resources it records must be deleted manually.".format(
                    os.path.abspath(state_file), e)) from e
        self.vpc_id = vpc_id
        self.sg_id = sg_id
        self.sn_ids = sn_ids
        self.internet_gateway = internet_gateway
        self.route_table = route_table
        self.infrastructure_id = infrastructure_id
        logger.debug("Done reading state from the local state file.")

    def write_state_file(self):
        """Save information that must persist to a file.

        We do not want to create a new VPC and new identical security groups, so we save
        information about them in a file between runs.
        """
        state = {}
        state['vpcID'] = self.vpc_id
        state['sgID'] = self.sg_id
        state['snIDs'] = self.sn_ids
        state['igwID'] = self.internet_gateway
        state['rtbID'] = self.route_table
        state['infrastructureID'] = self.infrastructure_id
        state['instances'] = self.instances
        state["instanceState"] = self.instance_states
        with open(self.state_file, 'w') as fh:
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

        vpc_name = self.generate_aws_id()
        # Set before anything is created, so that every resource is tagged with it.
        self.infrastructure_id = vpc_name

        try:
            tag_spec = self.create_name_tag_spec('vpc', vpc_name)

            # We use a large VPC so that the cluster can get large
            vpc = self.ec2.create_vpc(
                CidrBlock='10.0.0.0/16',
                AmazonProvidedIpv6CidrBlock=False,
                TagSpecifications=tag_spec,
            )
        except Exception as e:
            # This failure will cause a full abort
            logger.error("{}\n".format(e))
            raise e

        # Each resource ID is recorded as soon as the resource exists, so that
        # teardown() can remove it if a later step fails.
        self.vpc_id = vpc.id

        # Attach internet gateway so that our cluster can
        # talk to the outside internet
        internet_gateway = self.ec2.create_internet_gateway(
            TagSpecifications=self.create_name_tag_spec('internet-gateway', vpc_name),
        )
        self.internet_gateway = internet_gateway.id
        internet_gateway.attach_to_vpc(VpcId=vpc.vpc_id)  # Returns None

        # Create and configure route table to allow proper traffic
        route_table = self.config_route_table(vpc, internet_gateway)

        # Get all avaliability zones
        availability_zones = self.client.describe_availability_zones()

        # go through AZs and set up a subnet per
        for num, zone in enumerate(availability_zones['AvailabilityZones']):
            if zone['State'] == "available":
                zone_name = zone['ZoneName']
                tag_spec = self.create_name_tag_spec('subnet', '{0}.{1}'.format(vpc_name, zone_name))

                # Create a large subnet (4000 max nodes)
                subnet = vpc.create_subnet(
                    CidrBlock='10.0.{}.0/20'.format(16 * num),
                    AvailabilityZone=zone_name,
                    TagSpecifications=tag_spec,
                )
                self.sn_ids.append(subnet.id)

                # Make subnet accessible
                subnet.meta.client.modify_subnet_attribute(
                    SubnetId=subnet.id, MapPublicIpOnLaunch={"Value": True}
                )

                route_table.associate_with_subnet(SubnetId=subnet.id)
            else:
                logger.info("{} unavailable".format(zone['ZoneName']))
        # Security groups
        security_group_iterator = vpc.security_groups.all()
        for security_group in security_group_iterator:
            # this can only be the default security group for this VPC,
            # since no other security groups have been created yet
            security_group.create_tags(
                Tags=[{'Key': 'Name', 'Value': '{0}.default'.format(vpc_name)}],
            )

        self.security_group(vpc, vpc_name)
        return vpc

    def security_group(self, vpc, name):
        """Create and configure a new security group.

        Allows all ICMP in, all TCP and UDP in within VPC.

        This security group is very open. It allows all incoming ping requests on all
        ports. It also allows all outgoing traffic on all ports. This can be limited by
        changing the allowed port ranges.

        Parameters
        ----------
        vpc : VPC instance
            VPC in which to set up security group.
        name : str
            Name tag for the newly created security group.
        """

        tag_spec = self.create_name_tag_spec('security-group', name)

        sg = vpc.create_security_group(
            GroupName="private-subnet",
            Description="security group for remote executors",
            TagSpecifications=tag_spec,
        )
        self.sg_id = sg.id

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
        route_table = vpc.create_route_table(
            TagSpecifications=self.create_name_tag_spec('route-table', self.infrastructure_id),
        )
        self.route_table = route_table.id
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
        ami_id = self.image_id
        total_instances = len(self.instances)

        # not all availability zones support all instance types,
        # so we need to check which subnets can actually be used given the
        # user-provided instance type

        # this finds the availability zones in a region that support
        # the user-provided instance type
        az_offerings = self.client.describe_instance_type_offerings(
            LocationType='availability-zone',
            Filters=[
                {
                    'Name': 'instance-type',
                    'Values': [
                        instance_type,
                    ]
                },
            ],
        )

        allowed_azs = []
        for az in az_offerings['InstanceTypeOfferings']:
            allowed_azs.append(az['Location'])

        # this is used to retrieve the availability zone of each subnet
        subnets = self.client.describe_subnets(
            SubnetIds=list(self.sn_ids),
        )

        subnet = None
        for sn in subnets['Subnets']:
            sn_id = sn['SubnetId']
            az = sn['AvailabilityZone']
            # a subnet has been found with an availability zone
            # that is supported for the instance type, so we should use
            # this subnet
            if az in allowed_azs:
                subnet = sn_id
                break

        if subnet is None:
            logger.error("No subnet in region {0} supports instance type {1}".format(self.region, instance_type))
            return [None]

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
            tag_spec = self.create_name_tag_spec('instance', job_name)

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
                UserData=command,
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
        if not instances:
            instances = self.instances
        if not instances:
            # describe_instances with an empty InstanceIds list would describe
            # every instance in the account, not just ours.
            return self.instance_states
        desc = self.client.describe_instances(InstanceIds=instances)
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
                instance_status = JobStatus(instance_state)
                self.resources[instance_id]['status'] = instance_status
                all_states.extend([instance_status])

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

        job_name = self.generate_aws_id()
        wrapped_cmd = self.launcher(command,
                                    tasks_per_node,
                                    self.nodes_per_block,
                                    self.script_dir)
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

        job_ids = list(job_ids)
        if not job_ids:
            # terminate_instances rejects an empty InstanceIds list.
            return []

        if self.linger is True:
            logger.debug("Ignoring cancel requests due to linger mode")
            return [False for x in job_ids]

        try:
            self.client.terminate_instances(InstanceIds=job_ids)
        except Exception as e:
            logger.error("Caught error while attempting to remove instances: {0}".format(job_ids))
            raise e
        else:
            logger.debug("Removed the instances: {0}".format(job_ids))

        for job_id in job_ids:
            if job_id in self.resources:
                self.resources[job_id]["status"] = JobStatus(JobState.COMPLETED)
            if job_id in self.instances:
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
        """Teardown the EC2 infastructure created by this provider.

        Only resources which carry the ``parsl:infrastructure`` tag matching the
        infrastructure ID in the state file are deleted:

        1. Instances tagged with the infrastructure ID, or launched by this provider
           object, are terminated, and teardown waits for them to terminate.
        2. If any other instances remain in the VPC, they are left running and the
           VPC and its resources are kept.
        3. Otherwise the security group is deleted, the internet gateway detached
           and deleted, then the subnets, the route table and finally the VPC are
           deleted, and the state file is removed.

        Resources which are not tagged as created by this provider, including all
        infrastructure recorded by older versions of Parsl which did not tag
        resources, are left in place with a warning and the state file is kept.
        Resources which no longer exist are skipped, so teardown can be re-run
        after a partial failure. Any other EC2 error is raised and the state file
        is kept.

        This is not called automatically when Parsl shuts down; call it explicitly
        once the provider is no longer needed.

        Returns
        -------
        bool
            True if all of the infrastructure was removed, False if some was left.
        """

        if not self.vpc_id:
            logger.info("No VPC recorded; nothing to tear down")
            self._remove_state_file()
            return True

        vpc_filter = {'Name': 'vpc-id', 'Values': [self.vpc_id]}

        # Instances must be fully terminated before the security group,
        # internet gateway and subnets can be deleted.
        owned_queries = []
        if self.infrastructure_id:
            owned_queries.append([vpc_filter, {'Name': 'tag:' + INFRASTRUCTURE_TAG_KEY,
                                               'Values': [self.infrastructure_id]}])
        if self.instances:
            owned_queries.append([{'Name': 'instance-id', 'Values': list(self.instances)}])
        instance_ids = []
        for filters in owned_queries:
            for instance_id in self._live_instance_ids(filters):
                if instance_id not in instance_ids:
                    instance_ids.append(instance_id)
        if instance_ids:
            logger.info("Terminating instances: {}".format(instance_ids))
            self.client.terminate_instances(InstanceIds=instance_ids)
            self.client.get_waiter('instance_terminated').wait(InstanceIds=instance_ids)
        self.instances = []

        others = self._live_instance_ids([vpc_filter])
        if others:
            logger.warning("Not removing VPC {} or its resources: it contains instances which are not "
                           "tagged {}={} as launched by this provider, and which have been left running: "
                           "{}. Terminate them, then call teardown() again.".format(
                               self.vpc_id, INFRASTRUCTURE_TAG_KEY, self.infrastructure_id, others))
            return False

        vpc_ownership = self._ownership('VPC', self.vpc_id)
        if vpc_ownership == 'foreign' and not self.infrastructure_id:
            logger.warning(
                "Not removing EC2 infrastructure recorded in state file {}: the state file has no "
                "infrastructure ID, so it was written by an older version of Parsl (which did not "
                "tag the resources it created) or by hand, and the resources cannot be confirmed as "
                "created by this provider. Left in place: VPC {}, security group {}, internet gateway "
                "{}, route table {}, subnets {}. If they are no longer needed, delete them manually "
                "(for example with the AWS console or CLI: the security group, then detach and delete "
                "the internet gateway, then the subnets, the non-main route tables and the VPC), "
                "then remove the state file.".format(
                    os.path.abspath(self.state_file), self.vpc_id, self.sg_id,
                    self.internet_gateway or 'unknown', self.route_table or 'unknown', self.sn_ids))
            return False

        if vpc_ownership == 'missing':
            logger.info("VPC {} no longer exists".format(self.vpc_id))
        elif vpc_ownership == 'foreign':
            logger.warning(
                "Not removing VPC {} or the resources recorded with it in state file {}: the VPC is not "
                "tagged {}={}, so it was not created by this provider. Left in place: security group "
                "{}, internet gateway {}, route table {}, subnets {}.".format(
                    self.vpc_id, os.path.abspath(self.state_file), INFRASTRUCTURE_TAG_KEY,
                    self.infrastructure_id, self.sg_id, self.internet_gateway, self.route_table,
                    self.sn_ids))
            return False
        else:
            if not self._delete_owned_vpc_resources():
                return False
            self._delete_if_exists("VPC " + self.vpc_id, self.client.delete_vpc, VpcId=self.vpc_id)

        self.vpc_id = None
        self.sg_id = None
        self.internet_gateway = None
        self.route_table = None
        self.sn_ids = []
        logger.info("EC2 infrastructure torn down")
        self._remove_state_file()
        return True

    def _delete_owned_vpc_resources(self):
        """Delete the resources inside the VPC which carry this provider's tag.

        Returns False if any of them exist but are not tagged as created by this
        provider, in which case the VPC cannot be deleted.
        """
        left = []

        def remove(kind, resource_id, *deletes):
            """Run deletes if the resource is owned; return False if it is left in place."""
            ownership = self._ownership(kind, resource_id)
            if ownership == 'foreign':
                left.append("{} {}".format(kind, resource_id))
                return False
            if ownership == 'owned':
                for description, func, kwargs in deletes:
                    self._delete_if_exists(description, func, **kwargs)
            return True

        if self.sg_id and remove('security group', self.sg_id,
                                 ("security group " + self.sg_id, self.client.delete_security_group,
                                  {'GroupId': self.sg_id})):
            self.sg_id = None

        igw_id = self.internet_gateway
        if igw_id and remove('internet gateway', igw_id,
                             ("internet gateway {} attachment".format(igw_id),
                              self.client.detach_internet_gateway,
                              {'InternetGatewayId': igw_id, 'VpcId': self.vpc_id}),
                             ("internet gateway " + igw_id, self.client.delete_internet_gateway,
                              {'InternetGatewayId': igw_id})):
            self.internet_gateway = None

        # Deleting the subnets removes their route table associations,
        # which must happen before the route table can be deleted.
        for subnet in list(self.sn_ids):
            if remove('subnet', subnet, ("subnet " + subnet, self.client.delete_subnet, {'SubnetId': subnet})):
                self.sn_ids.remove(subnet)

        # The VPC's main route table and default security group are deleted with the VPC.
        rtb_id = self.route_table
        if rtb_id and remove('route table', rtb_id,
                             ("route table " + rtb_id, self.client.delete_route_table,
                              {'RouteTableId': rtb_id})):
            self.route_table = None

        if left:
            logger.warning("Not removing VPC {}: these resources recorded with it in state file {} are "
                           "not tagged {}={}, so were not created by this provider, and have been left "
                           "in place: {}".format(self.vpc_id, os.path.abspath(self.state_file),
                                                 INFRASTRUCTURE_TAG_KEY, self.infrastructure_id,
                                                 ", ".join(left)))
            return False
        return True

    def _live_instance_ids(self, filters):
        """Return the IDs of instances matching filters which are not terminated."""
        instance_ids = []
        paginator = self.client.get_paginator('describe_instances')
        pages = paginator.paginate(Filters=filters + [{
            'Name': 'instance-state-name',
            'Values': ['pending', 'running', 'shutting-down', 'stopping', 'stopped'],
        }])
        for page in pages:
            for reservation in page['Reservations']:
                for instance in reservation['Instances']:
                    instance_ids.append(instance['InstanceId'])
        return instance_ids

    def _ownership(self, kind, resource_id):
        """Check whether an EC2 resource carries this provider's ownership tag.

        Returns 'owned', 'foreign' (exists, but not tagged with this provider's
        infrastructure ID) or 'missing' (does not exist).
        """
        method, id_param, result_key = _DESCRIBE_RESOURCE[kind]
        try:
            resources = getattr(self.client, method)(**{id_param: [resource_id]})[result_key]
        except ClientError as e:
            if e.response.get('Error', {}).get('Code', '').endswith('.NotFound'):
                return 'missing'
            raise
        if not resources:
            return 'missing'
        tags = {tag['Key']: tag['Value'] for tag in resources[0].get('Tags', [])}
        if self.infrastructure_id and tags.get(INFRASTRUCTURE_TAG_KEY) == self.infrastructure_id:
            return 'owned'
        return 'foreign'

    def _delete_if_exists(self, description, func, **kwargs):
        """Call an EC2 delete/detach function, ignoring resources which are already gone."""
        try:
            func(**kwargs)
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code.endswith('.NotFound') or code == 'Gateway.NotAttached':
                logger.info("Skipping {}: {}".format(description, code))
            else:
                logger.error("Failed to delete {}: {}".format(description, e))
                raise
        else:
            logger.debug("Deleted {}".format(description))

    def _remove_state_file(self):
        try:
            os.remove(self.state_file)
        except FileNotFoundError:
            pass

    def generate_aws_id(self):
        """Generate a new ID for AWS resources.

        Returns
        -------
        str
            An ID of the form 'parsl.aws.123456.789' for giving resources unique identifiers.
        """
        return "parsl.aws.{0}".format(time.time())

    def create_name_tag_spec(self, resource_type, name):
        """Create a new tag specification for a resource name.

        As well as the Name tag, this includes the ``parsl:provider`` tag and, once
        the infrastructure ID is known, the ``parsl:infrastructure`` ownership tag.

        Parameters
        ----------
        resource_type : str
            The AWS resource type
        name : str
            The name to assign to the resource

        Returns
        -------
        record
            A TagSpecifications record to be passed into the creation of a new AWS resource.
        """
        tags = [{'Key': 'Name', 'Value': name},
                {'Key': PROVIDER_TAG_KEY, 'Value': 'AWSProvider'}]
        if self.infrastructure_id:
            tags.append({'Key': INFRASTRUCTURE_TAG_KEY, 'Value': self.infrastructure_id})
        return [{"ResourceType": resource_type, "Tags": tags}]

    @property
    def label(self):
        return self._label

    def goodbye(self):
        self.teardown()

    @property
    def status_polling_interval(self):
        return 60
