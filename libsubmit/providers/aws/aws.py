import os
import pprint
import math
import json
import time
import logging
import atexit
from datetime import datetime, timedelta
from string import Template
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.launchers import Launchers
from libsubmit.error import *
from libsubmit.providers.aws.template import template_string

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError

except ImportError:
    _boto_enabled = False
else:
    _boto_enabled = True

translate_table = {'pending': 'PENDING',
                   'running': 'RUNNING',
                   'terminated': 'COMPLETED',
                   'shutting-down': 'COMPLETED',  # (configuring),
                   'stopping': 'COMPLETED',  # We shouldn't really see this state
                   'stopped': 'COMPLETED',  # We shouldn't really see this state
                   }


class EC2Provider(ExecutionProvider):
    '''
    Here's a sample config for the EC2 provider:

    .. code-block:: python

         { "auth" : { # Definition of authentication method for AWS. One of 3 methods are required to authenticate
                      # with AWS : keyfile, profile or env_variables. If keyfile or profile is not set Boto3 will
                      # look for the following env variables :
                      # AWS_ACCESS_KEY_ID : The access key for your AWS account.
                      # AWS_SECRET_ACCESS_KEY : The secret key for your AWS account.
                      # AWS_SESSION_TOKEN : The session key for your AWS account.

              "keyfile"    : #{Description: Path to json file that contains 'AWSAccessKeyId' and 'AWSSecretKey'
                             # Type : String,
                             # Required : False},

              "profile"    : #{Description: Specify the profile to be used from the standard aws config file
                             # ~/.aws/config.
                             # Type : String,
                             # Expected : "default", # Use the 'default' aws profile
                             # Required : False},

            },

           "execution" : { # Definition of all execution aspects of a site

              "executor"   : #{Description: Define the executor used as task executor,
                             # Type : String,
                             # Expected : "ipp",
                             # Required : True},

              "provider"   : #{Description : The provider name, in this case ec2
                             # Type : String,
                             # Expected : "aws",
                             # Required :  True },

              "block" : { # Definition of a block

                  "nodes"      : #{Description : # of nodes to provision per block
                                 # Type : Integer,
                                 # Default: 1},

                  "taskBlocks" : #{Description : # of workers to launch per block
                                 # as either an number or as a bash expression.
                                 # for eg, "1" , "$(($CORES / 2))"
                                 # Type : String,
                                 #  Default: "1" },

                  "walltime"  :  #{Description : Walltime requested per block in HH:MM:SS
                                 # Type : String,
                                 # Default : "00:20:00" },

                  "initBlocks" : #{Description : # of blocks to provision at the start of
                                 # the DFK
                                 # Type : Integer
                                 # Default : ?
                                 # Required :    },

                  "minBlocks" :  #{Description : Minimum # of blocks outstanding at any time
                                 # WARNING :: Not Implemented
                                 # Type : Integer
                                 # Default : 0 },

                  "maxBlocks" :  #{Description : Maximum # Of blocks outstanding at any time
                                 # WARNING :: Not Implemented
                                 # Type : Integer
                                 # Default : ? },

                  "options"   : {  # Scheduler specific options


                      "instanceType" : #{Description : Instance type t2.small|t2...
                                       # Type : String,
                                       # Required : False
                                       # Default : t2.small },

                      "imageId"      : #{"Description : String to append to the #SBATCH blocks
                                       # in the submit script to the scheduler
                                       # Type : String,
                                       # Required : False },

                      "region"       : #{"Description : AWS region to launch machines in
                                       # in the submit script to the scheduler
                                       # Type : String,
                                       # Default : 'us-east-2',
                                       # Required : False },

                      "keyName"      : #{"Description : Name of the AWS private key (.pem file)
                                       # that is usually generated on the console to allow ssh access
                                       # to the EC2 instances, mostly for debugging.
                                       # in the submit script to the scheduler
                                       # Type : String,
                                       # Required : True },

                      "spotMaxBid"   : #{"Description : If requesting spot market machines, specify
                                       # the max Bid price.
                                       # Type : Float,
                                       # Required : False },
                  }
              }
            }
         }
    '''

    def __repr__(self):
        return "<EC2 Execution Provider for site:{0}>".format(self.sitename)

    def __init__(self, config, channel=None):
        ''' Initialize the EC2Provider class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs:
             - Channel (None): A channel is not required for EC2.

        '''

        self.channel = channel
        if not _boto_enabled:
            raise OptionalModuleMissing(
                ['boto3'], "AWS Provider requires boto3 module.")

        self.config = config
        self.sitename = config['site']
        self.current_blocksize = 0
        self.resources = {}
        # atexit.register(self.goodbye)
        self.config = config
        options = self.config["execution"]["block"]["options"]
        logger.warn("Options %s", options)
        self.instance_type = options.get("instanceType", "t2.small")
        self.image_id = options["imageId"]
        self.key_name = options["keyName"]
        self.region = options.get("region", 'us-east-2')
        self.max_nodes = (self.config["execution"]["block"].get("maxBlocks", 1) *
                          self.config["execution"]["block"].get("nodes", 1))

        self.spot_max_bid = options.get("spotMaxBid", 0)

        try:
            self.initialize_boto_client()
        except Exception as e:
            logger.error("Site:[{0}] Failed to initialize".format(self))
            raise e

        try:
            self.statefile = self.config["execution"]["block"]["options"].get("stateFile",
                                                                              '.ec2site_{0}.json'.format(self.sitename))
            self.read_state_file(self.statefile)

        except Exception as e:
            self.create_vpc().id
            logger.info(
                "No State File. Cannot load previous options. Creating new infrastructure")
            self.write_state_file()

    @property
    def channels_required(self):
        ''' No channel required for EC2
        '''
        return False

    def initialize_boto_client(self):
        ''' Use auth configs to initialize the boto client
        '''

        self.sitename = self.config['site']
        self.session = self.create_session()
        self.client = self.session.client('ec2')
        self.ec2 = self.session.resource('ec2')
        self.instances = []
        self.instance_states = {}
        self.vpc_id = 0
        self.sg_id = 0
        self.sn_ids = []

    def read_state_file(self, statefile):
        """If this script has been run previously, it will be persisitent
           by writing resource ids to state file. On run, 
           the script looks for a state file
           before creating new infrastructure

           Information on VPCs, security groups, and subnets are saved,
           as well as running instances and their states.

           AWS has a maximum number of VPCs per region per account so we don't
           want to clutter users' aws accounts with security groups and vpcs that
           will be used only once.

        """
        try:
            fh = open(statefile, 'r')
            state = json.load(fh)
            self.vpc_id = state['vpcID']
            self.sg_id = state['sgID']
            self.sn_ids = state['snIDs']
            self.instances = state['instances']
        except Exception as e:
            logger.debug(
                "Caught exception while reading state file: {0}".format(e))
            raise e
        logger.debug("Done reading state from the local state file")

    def write_state_file(self):
        ''' Save information that must persist to a file.
            We don't want to create a new vpc and new identical 
            security groups, so we save information about them 
            in a file between runs
        '''
        fh = open('awsproviderstate.json', 'w')
        state = {}
        state['vpcID'] = self.vpc_id
        state['sgID'] = self.sg_id
        state['snIDs'] = self.sn_ids
        state['instances'] = self.instances
        state["instanceState"] = self.instance_states
        fh.write(json.dumps(state, indent=4))

    def create_session(self):
        ''' Here we will first look in the ~/.aws/config file.

        First we look in config["auth"]["keyfile"] for a path to a json file
        with the credentials.
        the keyfile should have 'AWSAccessKeyId' and 'AWSSecretKey'

        Next we look for config["auth"]["profile"] for a profile name and try
        to use the Session call to auto pick up the keys for the profile from
        the user default keys file ~/.aws/config.

        Lastly boto3 will look for the keys in env variables:
        AWS_ACCESS_KEY_ID : The access key for your AWS account.
        AWS_SECRET_ACCESS_KEY : The secret key for your AWS account.
        AWS_SESSION_TOKEN : The session key for your AWS account.
        This is only needed when you are using temporary credentials.
        The AWS_SECURITY_TOKEN environment variable can also be used,
        but is only supported for backwards compatibility purposes.
        AWS_SESSION_TOKEN is supported by multiple AWS SDKs besides python.
        '''

        session = None

        if "keyfile" in self.config["auth"]:
            c = self.config["auth"]["keyfile"]
            credfile = os.path.expandvars(os.path.expanduser(c))

            try:
                with open(credfile, 'r') as f:
                    creds = json.load(credfile)
            except json.JSONDecodeError as e:
                logger.error("Site[{0}]: Json decode error in credential file {1}".format(
                    self, credfile))
                raise e

            except Exception as e:
                logger.debug("Caught exception: {0} while reading credential file: {1}".format(
                    self, credfile))
                raise e

            logger.debug(
                "Site[{0}]: Using credential file to create session".format(self))
            session = boto3.session.Session(**creds, region_name=self.region)

        elif "profile" in self.config["auth"]:

            logger.debug(
                "Site[{0}]: Using profile name to create session".format(self))
            session = boto3.session.Session(profile_name=self.config["auth"]["profile"],
                                            region_name=self.region)

        elif (os.getenv("AWS_ACCESS_KEY_ID") is not None
              and os.getenv("AWS_SECRET_ACCESS_KEY") is not None):

            logger.debug(
                "Site[{0}]: Using env variables to create session".format(self))
            session = boto3.session.Session(region_name=self.region)

        else:
            logger.error(
                "Site[{0}]: Credentials not available to create session".format(self))
            session = boto3.session.Session(region_name=self.region)
            print(session)

        return session

    def create_vpc(self):
        ''' Create and configure VPC

            We create a VPC with CIDR 10.0.0.0/16, which provides
            up to 64,000 instances.

            We attach a subnet for each availability zone within the
            region specified in the config. We give each subnet an
            ip range like 10.0.X.0/20, which is large enough for
            approx. 4000 instances.

            Security groups are configured in function security_group.
            More information is there
        '''

        try:
            # We use a large VPC so that the cluster can get large
            vpc = self.ec2.create_vpc(
                CidrBlock='10.0.0.0/16',
                AmazonProvidedIpv6CidrBlock=False,
            )
        except Exception as e:
            # This failiure will cause a full abort
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
                    CidrBlock='10.0.{}.0/20'.format(16 * num),
                    AvailabilityZone=zone['ZoneName'])

                # Make subnet accessible
                subnet.meta.client.modify_subnet_attribute(
                    SubnetId=subnet.id, MapPublicIpOnLaunch={"Value": True})

                route_table.associate_with_subnet(SubnetId=subnet.id)
                self.sn_ids.append(subnet.id)
            else:
                logger.info("{} unavailable".format(zone['ZoneName']))
        # Security groups
        sg = self.security_group(vpc)
        self.vpc_id = vpc.id
        return vpc

    def security_group(self, vpc):
        ''' Create and configure security group.
        Allows all ICMP in, all tcp and udp in within vpc

        This security group is very open. It allows all
        incoming ping requests on all ports. It also allows
        all outgoing traffic on all ports. This can be limited
        by changing the allowed port ranges.

        :param vpc - vpc in which to set up security group
        '''

        sg = vpc.create_security_group(
            GroupName="private-subnet",
            Description="security group for remote executors")

        ip_ranges = [{
            'CidrIp': '172.32.0.0/16'
        }]

        # Allows all ICMP in, all tcp and udp in within vpc

        inPerms = [{
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
            'IpRanges': [{'CidrIp': '0.0.0.0/0'}],
        }]
        # Allows all TCP out, all tcp and udp out within vpc
        outPerms = [{
            'IpProtocol': 'TCP',
            'FromPort': 0,
            'ToPort': 65535,
            'IpRanges': [{'CidrIp': '0.0.0.0/0'}],
        }, {
            'IpProtocol': 'TCP',
            'FromPort': 0,
            'ToPort': 65535,
            'IpRanges': ip_ranges,
        }, {
            'IpProtocol': 'UDP',
            'FromPort': 0,
            'ToPort': 65535,
            'IpRanges': ip_ranges,
        }, ]

        sg.authorize_ingress(IpPermissions=inPerms)
        sg.authorize_egress(IpPermissions=outPerms)
        self.sg_id = sg.id
        return sg

    def config_route_table(self, vpc, internet_gateway):
        ''' Configure route table for vpc
        Args:
            :param vpc (dict) : dictionary representing the vpc 
                                created by create_vpc()
            :param internet_gateway (dict) : dictionary representing igw
                                             created by create_vpc() 
        '''
        route_table = vpc.create_route_table()
        route_ig_ipv4 = route_table.create_route(
            DestinationCidrBlock='0.0.0.0/0',
            GatewayId=internet_gateway.internet_gateway_id)
        return route_table

    def xstr(self, s):
        return '' if s is None else s

    def spin_up_instance(self, cmd_string, job_name):
        ''' Starts an instance in the vpc in first available
        subnet. Starts up n instances if nodes per block > 1
        Not supported. We only do 1 node per block

        Args:
            - cmd_string (str) : Command string to execute on the node
            - job_name (str) : Name associated with the instances

        '''

        escaped_command = self.xstr(cmd_string)
        command = Template(template_string).substitute(jobname=job_name,
                                                       user_script=cmd_string)
        instance_type = self.instance_type
        subnet = self.sn_ids[0]
        ami_id = self.image_id
        total_instances = len(self.instances)

        if float(self.spot_max_bid) > 0:
            spot_options = {'MarketType': 'spot',
                            'SpotOptions': {
                                'MaxPrice': str(self.spot_max_bid),
                                'SpotInstanceType': 'one-time',
                                'InstanceInterruptionBehavior': 'terminate'
                            }
                            }
        else:
            spot_options = {}

        if total_instances > self.max_nodes:
            logger.warn(
                "Exceeded instance limit ({}). Cannot Continue\n".format(self.max_nodes))
            return [None]
        try:
            tag_spec = [{"ResourceType": "instance",
                         "Tags": [{'Key': 'Name',
                                   'Value': job_name}]}]

            instance = self.ec2.create_instances(MinCount=1,
                                                 MaxCount=1,
                                                 InstanceType=instance_type,
                                                 ImageId=ami_id,
                                                 KeyName=self.key_name,
                                                 SubnetId=subnet,
                                                 SecurityGroupIds=[self.sg_id],
                                                 TagSpecifications=tag_spec,
                                                 InstanceMarketOptions=spot_options,
                                                 InstanceInitiatedShutdownBehavior='terminate',
                                                 UserData=command)
        except ClientError as e:
            print(e)
            logger.error(e.response)
            return [None]

        except Exception as e:
            logger.error("Request for EC2 resources failed : {0}".format(e))
            return [None]

        self.instances.append(instance[0].id)
        logger.info("Started up 1 instance {} . Instance type:{}".format(
            instance[0].id, instance_type))
        return instance

    def shut_down_instance(self, instances=None):
        ''' Shuts down a list of instances if provided or the last
        instance started up if none provided

        [TODO] ...
        '''

        if instances and len(self.instances) > 0:
            print(instances)
            try:
                print([i.id for i in instances])
            except Exception as e:
                print(e)
            term = self.client.terminate_instances(InstanceIds=instances)
            logger.info("Shut down {} instances (ids:{}".format(
                len(instances), str(instances)))
        elif len(self.instances) > 0:
            instance = self.instances.pop()
            term = self.client.terminate_instances(InstanceIds=[instance])
            logger.info("Shut down 1 instance (id:{})".format(instance))
        else:
            logger.warn("No Instances to shut down.\n")
            return -1
        self.get_instance_state()
        return term

    def get_instance_state(self, instances=None):
        """Get stateus of all instances on EC2 which were started by this
        file"""
        if instances:
            desc = self.client.describe_instances(InstanceIds=instances)
        else:
            desc = self.client.describe_instances(InstanceIds=self.instances)
        # pprint.pprint(desc['Reservations'],indent=4)
        for i in range(len(desc['Reservations'])):
            instance = desc['Reservations'][i]['Instances'][0]
            self.instance_states[instance['InstanceId']
                                 ] = instance['State']['Name']
        return self.instance_states

    def ipyparallel_configuration(self):
        config = ''
        try:
            with open(os.path.expanduser(self.config['iPyParallelConfigFile'])) as f:
                config = f.read().strip()
        except Exception as e:
            logger.error(e)
            logger.info(
                "Couldn't find user ipyparallel config file. Trying default location.")
            with open(os.path.expanduser("~/.ipython/profile_default/security/ipcontroller-engine.json")) as f:
                config = f.read().strip()
        else:
            logger.error(
                "Cannot find iPyParallel config file. Cannot proceed.")
            return -1
        ipptemplate = """
cat <<EOF > ipengine.json
{}
EOF

mkdir -p '.ipengine_logs'
sleep 5
ipengine --file=ipengine.json &> ipengine.log &
ipengine --file=ipengine.json &> ipengine.log &""".format(config)
        return ipptemplate

    #######################################################
    # Status
    #######################################################
    def status(self, job_ids):
        '''  Get the status of a list of jobs identified by their ids.

        Args:
            - job_ids (List of ids) : List of identifiers for the jobs

        Returns:
            - List of status codes.
        '''

        all_states = []
        for job_id in job_ids:
            try:
                if job_id in self.resources:
                    self.ec2.Instance(job_id)
                    print("State : ", self.resources[job_id]["instance"].state)
                    print("Reason : ",
                          self.resources[job_id]["instance"].state_reason)
                    # self.resources[job_id]["instance"].update()
                    s = self.resources[job_id]["instance"].state['Name']
                    state_string = translate_table.get(s, 'UNKNOWN')
                    all_states.extend([state_string])

            except Exception as e:
                logger.warn('Caught exception : {0}'.format(e))
                logger.warn(
                    'Could not get state of instance:{0}'.format(job_id))
                all_states.extend(['UNKNOWN'])

        return all_states

    ########################################################
    # Submit
    ########################################################
    def submit(self, cmd_string='sleep 1', blocksize=1, job_name="parsl.auto"):
        '''Submits the cmd_string onto a freshly instantiated AWS EC2 instance.
        Submit returns an ID that corresponds to the task that was just submitted.

        Args:
             - cmd_string (str): Commandline invocation to be made on the remote side.
             - blocksize (int) : Number of blocks requested

        Kwargs:
             - job_name (String): Prefix for job name

        Returns:
             - None: At capacity, cannot provision more
             - job_id: (string) Identifier for the job

        '''

        job_name = "parsl.auto.{0}".format(time.time())
        [instance, *rest] = self.spin_up_instance(cmd_string=cmd_string,
                                                  job_name=job_name)

        if not instance:
            logger.error("Failed to submit request to EC2")
            return None

        logger.debug("Started instance_id : {0}".format(instance.instance_id))

        state = translate_table.get(instance.state['Name'], "PENDING")

        self.resources[instance.instance_id] = {"job_id": instance.instance_id,
                                                "instance": instance,
                                                "status": state}

        return instance.instance_id

    ########################################################
    # Cancel
    ########################################################
    def cancel(self, job_ids):
        ''' Cancels the jobs specified by a list of job ids

        Args:
             job_ids (list) : List of of job identifiers

        Returns :
             [True/False...] : If the cancel operation fails the entire list will be False.
        '''

        try:
            status = self.client.terminate_instances(InstanceIds=list(job_ids))
        except Exception as e:
            logger.error(
                "Caught error while attempting to remove instances: {0}".format(job_ids))
            raise e
        else:
            logger.debug("Removed the instances : {0}".format(job_ids))

        for job_id in job_ids:
            self.resources[job_id]["status"] = "COMPLETED"

        for job_id in job_ids:
            self.instances.remove(job_id)

        return [True for x in job_ids]

    def show_summary(self):
        """Print human readable summary of current
           AWS state to log and to console
        """
        self.get_instance_state()
        status_string = "EC2 Summary:\n\tVPC IDs: {}\n\tSubnet IDs: \
{}\n\tSecurity Group ID: {}\n\tRunning Instance IDs: {}\n".format(
            self.vpc_id,
            self.sn_ids,
            self.sg_id,
            self.instances)
        status_string += "\tInstance States:\n\t\t"
        self.get_instance_state()
        for state in self.instance_states.keys():
            status_string += "Instance ID: {}  State: {}\n\t\t".format(
                state, self.instance_states[state])
        status_string += "\n"
        logger.info(status_string)
        return status_string

    def teardown(self):
        """Terminate all EC2 instances, delete all subnets,
           delete security group, delete vpc
           and reset all instance variables
        """

        self.shut_down_instance(self.instances)
        self.instances = []
        try:
            self.client.delete_internet_gateway(
                InternetGatewayId=self.internet_gateway)
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
        os.remove(self.config['stateFilePath'])

    @property
    def scaling_enabled():
        return True

    @property
    def current_capacity(self):
        ''' Returns the current blocksize.
        This may need to return more information in the futures :
        { minsize, maxsize, current_requested }
        '''
        return len(self.instances)

    def goodbye(self):
        self.teardown()
