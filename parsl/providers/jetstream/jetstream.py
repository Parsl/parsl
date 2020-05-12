import ast
import logging
import os

from parsl.providers.error import OptionalModuleMissing
try:
    from novaclient import api_versions
    from novaclient import client

except ImportError:
    _nova_enabled = False
else:
    _nova_enabled = True

logger = logging.getLogger(__name__)

setup_script = '''#!/bin/bash
LOG=/root/userdata.logs
echo "Userdata logs " > $LOG
apt-get update  &>> $LOG
yes | aptdcon --hide-terminal --install python3-pip &>> $LOG
pip3 install jupyter ipyparallel parsl &>> $LOG
cat <<EOF > ipcontroller-engine.json
{engine_config}
EOF
ipengine --file=ipcontroller-engine.json &>> $LOG
'''


class JetstreamProvider(object):
    def __init__(self, config, poolname):
        self.config = config
        self.blocks = {}
        self.pool = poolname
        controller_file = "~/.ipython/profile_default/security/ipcontroller-engine.json"

        if not _nova_enabled:
            raise OptionalModuleMissing(['python-novaclient'],
                                        "Jetstream Provider requires the python-novaclient module.")

        self.client = client.Client(
            api_versions.APIVersion("2.0"),
            config['sites.jetstream']['OS_USERNAME'],
            config['sites.jetstream']['OS_PASSWORD'],
            project_id=config['sites.jetstream']['OS_PROJECT_ID'],
            project_name=config['sites.jetstream']['OS_PROJECT_NAME'],
            auth_url=config['sites.jetstream']['OS_AUTH_URL'],
            insecure=False,
            region_name=config['sites.jetstream']['OS_REGION_NAME'],
            user_domain_name=config['sites.jetstream']['OS_USER_DOMAIN_NAME'])

        api_version = api_versions.get_api_version("2.0")
        api_version = api_versions.discover_version(self.client, api_version)
        client.discover_extensions(api_version)

        logger.debug(self.client.has_neutron())
        self.server_manager = self.client.servers

        try:
            with open(os.path.expanduser(controller_file), 'r') as f:
                self.engine_config = f.read()

        except FileNotFoundError:
            logger.error("No controller_file found at : %s. Cannot proceed", controller_file)
            exit(-1)

        except Exception as e:

            logger.error("Caught exception while reading from the ipcontroller_engine.json")
            raise e

        try:
            # Check if the authentication worked by forcing a call
            self.server_manager.list()

        except Exception as e:
            logger.error("Caught exception : %s", e)
            raise e

        flavors = self.client.flavors.list()

        try:
            self.flavor = [f for f in flavors if f.name == config['sites.jetstream.{0}'.format(poolname)]['flavor']][0]
        except Exception as e:
            logger.error("Caught exception : ", e)
            raise e

        self.sec_groups = ast.literal_eval(config['sites.jetstream.{0}'.format(poolname)]['sec_groups'])
        self.nics = ast.literal_eval(config['sites.jetstream.{0}'.format(poolname)]['nics'])

    def scale_out(self, blocks=1, block_size=1):
        ''' Scale out the existing resources.
        '''
        self.config['sites.jetstream.{0}'.format(self.pool)]['flavor']
        count = 0
        if blocks == 1:
            block_id = len(self.blocks)
            self.blocks[block_id] = []
            for instance_id in range(0, block_size):
                instances = self.server_manager.create(
                    'parsl-{0}-{1}'.format(block_id, instance_id),  # Name
                    self.client.images.get('87e08a17-eae2-4ce4-9051-c561d9a54bde'),  # Image_id
                    self.client.flavors.list()[0],
                    min_count=1,
                    max_count=1,
                    userdata=setup_script.format(engine_config=self.engine_config),
                    key_name='TG-MCB090174-api-key',
                    security_groups=['global-ssh'],
                    nics=[{
                        "net-id": '724a50cf-7f11-4b3b-a884-cd7e6850e39e',
                        "net-name": 'PARSL-priv-net',
                        "v4-fixed-ip": ''
                    }])
                self.blocks[block_id].extend([instances])
                count += 1

        return count

    def scale_in(self, blocks=0, machines=0, strategy=None):
        ''' Scale in resources
        '''
        count = 0
        instances = self.client.servers.list()
        for instance in instances[0:machines]:
            print("Deleting : ", instance)
            instance.delete()
            count += 1

        return count

    @property
    def status_polling_interval(self):
        return 60
