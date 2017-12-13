import os
import pprint
import math
import json
import time
import logging
import atexit
from string import Template
from libsubmit.providers.provider_base import ExecutionProvider
from libsubmit.launchers import Launchers
from libsubmit.error import *
from libsubmit.providers.aws.template import template_string

logger = logging.getLogger(__name__)

try :
    import boto3
    from botocore.exceptions import ClientError

except ImportError:
    _boto_enabled = False
else:
    _boto_enabled = True


AWS_REGIONS = ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']

DEFAULT_REGION = 'us-east-2'

translate_table = {'PD': 'PENDING',
                   'R': 'RUNNING',
                   'CA': 'CANCELLED',
                   'CF': 'PENDING',  # (configuring),
                   'CG': 'RUNNING',  # (completing),
                   'CD': 'COMPLETED',
                   'F': 'FAILED',  # (failed),
                   'TO': 'TIMEOUT',  # (timeout),
                   'NF': 'FAILED',  # (node failure),
                   'RV': 'FAILED',  # (revoked) and
                   'SE': 'FAILED'}  # (special exit state



class EC2Provider(ExecutionProvider):
    '''
    Here's a sample config for the EC2 provider:

    .. code-block:: python

         { "execution" : { # Definition of all execution aspects of a site

              "executor"   : #{Description: Define the executor used as task executor,
                             # Type : String,
                             # Expected : "ipp",
                             # Required : True},

              "provider"   : #{Description : The provider name, in this case ec2
                             # Type : String,
                             # Expected : "slurm",
                             # Required :  True },

              "script_dir" : #{Description : Relative or absolute path to a
                             # directory in which intermediate scripts are placed
                             # Type : String,
                             # Default : "./.scripts"},

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

                      "imageId" : #{"Description : String to append to the #SBATCH blocks
                                    # in the submit script to the scheduler
                                    # Type : String,
                                    # Required : False },
                  }
              }
            }
         }
    '''


    def __repr__ (self):
        return "<EC2 Execution Provider for site:{0}>".format(self.sitename)

    def __init__(self, config, channel=None):
        ''' Initialize the EC2Provider class

        Args:
             - Config (dict): Dictionary with all the config options.

        KWargs:
             - Channel (None): A channel is not required for EC2.

        '''

        self.channel = channel
        if not _boto_enabled :
            raise OptionalModuleMissing(['boto3'], "AWS Provider requires boto3 module.")

        self.config = config
        self.sitename = config['site']
        self.current_blocksize = 0
        self.resources = {}

        self.config = config
        options = self.config["execution"]["block"]["options"]
        logger.warn("Options %s", options)
        self.instance_type = options.get("instanceType", "t2.small")
        self.image_id      = options["imageId"]
        self.key_name      = options["keyName"]
        self.max_nodes     = (self.config["execution"]["block"].get("maxBlocks",1)*
        self.config["execution"]["block"].get("nodes", 1))
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
            logger.info("No State File. Cannot load previous options. Creating new infrastructure")
            self.write_state_file()

    @property
    def channels_required(self):
        return False

    def initialize_boto_client(self):
        ''' Use auth configs to initialize the boto client
        '''

        self.sitename = self.config['site']
        self.session  = self.create_session()
        self.client = self.session.client('ec2')
        self.ec2 = self.session.resource('ec2')
        self.instances = []
        self.instance_states = {}
        self.vpc_id = 0
        self.sg_id = 0
        self.sn_ids = []

    def read_state_file(self, statefile):
        """If this script has been run previously, it will be persisitent
        by writing resource ids to state file. On run, the script looks for a state file
        before creating new infrastructure"""
        try:
            fh = open(statefile, 'r')
            state = json.load(fh)
            self.vpc_id = state['vpcID']
            self.sg_id = state['sgID']
            self.sn_ids = state['snIDs']
            self.instances = state['instances']
        except Exception as e:
            logger.debug("Caught exception while reading state file: {0}".format(e))
            raise e
        logger.debug("Done reading state from the local state file")

    def write_state_file(self):
        fh = open('awsproviderstate.json', 'w')
        state = {}
        state['vpcID'] = self.vpc_id
        state['sgID'] = self.sg_id
        state['snIDs'] = self.sn_ids
        state['instances'] = self.instances
        state["instanceState"] = self.instance_states
        fh.write(json.dumps(state, indent=4))

    def _read_conf(self, config_file):
        """read config file"""
        config = json.load(open(config_file, 'r'))
        return config

    def pretty_configs(self, configs):
        """prettyprint config"""
        printer = pprint.PrettyPrinter(indent=4)
        printer.pprint(configs)

    def read_configs(self, config_file):
        """Read config file"""
        config = self._read_conf(config_file)
        return config

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

        region = self.config["execution"]["block"]["options"].get("region", DEFAULT_REGION)

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
                logger.debug("Caught exception: {0} while reading credential file: {1}".format(self, credfile))
                raise e

            logger.debug("Site[{0}]: Using credential file to create session".format(self))
            session = boto3.session.Session(**creds, region_name=region)

        elif "profile" in self.config["auth"]:

            logger.debug("Site[{0}]: Using profile name to create session".format(self))
            session = boto3.session.Session(profile_name=self.config["auth"]["profile"],
                                            region_name=region)

        elif (os.getenv("AWS_ACCESS_KEY_ID") is not None
              and os.getenv("AWS_SECRET_ACCESS_KEY") is not None):

            logger.debug("Site[{0}]: Using env variables to create session".format(self))
            session = boto3.session.Session(region_name=region)

        else:
            logger.error("Site[{0}]: Credentials not available to create session".format(self))
            session = boto3.session.Session(region_name=region)
            print(session)

        return session

    def create_vpc(self):
        """Create and configure VPC"""
        try:
            vpc = self.ec2.create_vpc(
                CidrBlock='10.0.0.0/16',
                AmazonProvidedIpv6CidrBlock=False,
            )
        except Exception as e:
            logger.error("{}\n".format(e))
            raise e
        internet_gateway = self.ec2.create_internet_gateway()
        internet_gateway.attach_to_vpc(VpcId=vpc.vpc_id)  # Returns None
        self.internet_gateway = internet_gateway.id
        route_table = self.config_route_table(vpc, internet_gateway)
        self.route_table = route_table.id
        availability_zones = self.client.describe_availability_zones()
        for num, zone in enumerate(availability_zones['AvailabilityZones']):
            if zone['State'] == "available":
                subnet = vpc.create_subnet(
                    CidrBlock='10.0.{}.0/20'.format(16 * num),
                    AvailabilityZone=zone['ZoneName'])
                subnet.meta.client.modify_subnet_attribute(
                    SubnetId=subnet.id, MapPublicIpOnLaunch={"Value": True})
                route_table.associate_with_subnet(SubnetId=subnet.id)
                self.sn_ids.append(subnet.id)
            else:
                print("{} unavailable".format(zone['ZoneName']))
        # Security groups
        sg = self.security_group(vpc)
        self.vpc_id = vpc.id
        return vpc

    def security_group(self, vpc):
        """Create and configure security group.
        Allows all ICMP in, all tcp and udp in within vpc
        """
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
        """Configure route table for vpc"""
        route_table = vpc.create_route_table()
        route_ig_ipv4 = route_table.create_route(
            DestinationCidrBlock='0.0.0.0/0',
            GatewayId=internet_gateway.internet_gateway_id)
        return route_table

    def scale_out(self, size, cmd_string=None):
        """Scale cluster out (larger)"""
        job_name = "parsl.auto.{0}".format(time.time())
        instances = []
        for i in range(size):
            instances.append(self.spin_up_instance(cmd_string=cmd_string,
                                                   job_name=job_name))
        self.current_blocksize += size
        logger.info("started {} instances".format(size))
        return instances

    def scale_in(self, size):
        """Scale cluster in (smaller)"""
        for i in range(size):
            self.shut_down_instance()
        self.current_blocksize -= size

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

        if total_instances > self.max_nodes:
            logger.warn("You have requested more instances ({}) than your max_nodes ({}). Cannot Continue\n".format(total_instances, self.max_nodes))
            return -1
        instance = self.ec2.create_instances(
            InstanceType=instance_type,
            ImageId=ami_id,
            MinCount=1,
            MaxCount=1,
            KeyName=self.key_name,
            SubnetId=subnet,
            SecurityGroupIds=[self.sg_id],
            TagSpecifications = [{"ResourceType" : "instance",
                                  "Tags" : [{'Key' : 'Name',
                                             'Value' : job_name }]}
                ],
            UserData=command)
        self.instances.append(instance[0].id)
        logger.info(
            "Started up 1 instance. Instance type:{}".format(instance_type))
        return instance

    def shut_down_instance(self, instances=None):
        """Shuts down a list of instances if provided or the last
        instance started up if none provided"""
        if instances and len(self.instances) > 0:
            term = self.client.terminate_instances(InstanceIds=instances)
            logger.info(
                "Shut down {} instances (ids:{}".format(
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
        return self.get_instance_state()

    ########################################################
    # Submit
    ########################################################
    def submit(self, cmd_string='sleep 1', blocksize=1, job_name="parsl.auto"):
        """Submit an ipyparalel pilot job which will connect to an ipp controller specified
        by your ipp config file and run cmd_string on the instance being started up."""
        return self.scale_out(cmd_string=cmd_string, size=blocksize)

    ########################################################
    # Cancel
    ########################################################
    def cancel(self, job_ids):
        ''' Cancels the jobs specified by a list of job ids
        Args:
        job_ids : [<job_id> ...]
        Returns :
        [True/False...] : If the cancel operation fails the entire list will be False.
        '''
        return self.shut_down_instance(instances=job_ids)

    def show_summary(self):
        """Print human readable summary of current
        AWS state to log and to console"""
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

    def scaling_enabled():
        return True
    # @atexit.register
    # def goodbye():
    #     self.teardown()


if __name__ == '__main__':
    conf = "providerconf.json"
    provider = EC2Provider(conf)
    provider.submit("sleep 5")
    print(provider.show_summary())
    # provider.scale_in(1)
    print(provider.show_summary())
