"""
Specification of user-specific configuration options.

The fields must be configured separately for each user. To disable any associated configurations, comment
out the entry.

User specific overrides that should not go in version control can be set by creating a
file called local_user_opts.py, which declares a dictionary local_user_opts. Top level
keys in that dictionary will replace entries in the below user opts file, so it should
be safe to cut-and-paste entries from this file into that file.
"""
from typing import Any, Dict

# PUBLIC_IP = "52.86.208.63" # "128.135.250.229"
# MIDWAY_USERNAME = "yadunand"
# OSG_USERNAME = "yadunand"
# SWAN_USERNAME = "p02509"
# CORI_USERNAME = "yadunand"
# ALCF_USERNAME = "yadunand"
# ALCF_ALLOCATION = "CSC249ADCD01"
# COMET_USERNAME = "yadunand"

user_opts = {
    'frontera': {
        'worker_init': 'source ~/setup_parsl_test_env.sh;',
    },
    'nscc': {
        'worker_init': 'source ~/setup_parsl_test_env.sh;',
    },
    'theta': {
        'worker_init': 'source ~/setup_parsl_test_env.sh;',
    },
    'summit': {
        'worker_init': 'source ~/setup_parsl_test_env.sh;',
    },
    'bluewaters': {
        'worker_init': 'source ~/setup_parsl_test_env.sh;',
    },
    'midway': {
        'worker_init': 'source ~/setup_parsl_test_env.sh;',
    },
    'petrelkube': {
        'worker_init': '~/setup_parsl_test_env.sh',
    },
    # 'comet': {
    #     'username': COMET_USERNAME,
    #     'script_dir': '/home/{}/parsl_scripts'.format(COMET_USERNAME),
    #     'scheduler_options': "",
    #     'worker_init': 'export PATH:/home/{}/anaconda3/bin/:$PATH; source activate parsl_0.5.0_py3.6;'.format(COMET_USERNAME),
    # },
    # 'midway': {
    #     'username': MIDWAY_USERNAME,
    #     'script_dir': '/scratch/midway2/{}/parsl_scripts'.format(MIDWAY_USERNAME),
    #     'scheduler_options': "",
    #     'worker_init': 'cd /scratch/midway2/{}/parsl_scripts; '
    #                    'module load Anaconda3/5.1.0; source activate parsl_testing;'
    #                    .format(MIDWAY_USERNAME),
    # },
    # 'osg': {
    #     'username': OSG_USERNAME,
    #     'script_dir': '/home/{}/parsl_scripts'.format(OSG_USERNAME),
    #     'scheduler_options': "",
    #     'worker_init' : 'module load python/3.5.2; python3 -m venv parsl_env;
    #                      source parsl_env/bin/activate; python3 -m pip install parsl==0.5.2'
    # },
    # 'swan': {
    #     'username': SWAN_USERNAME,
    #     'script_dir' : "/home/users/{}/parsl_scripts".format(SWAN_USERNAME),
    #     'scheduler_options': "",
    #     'worker_init': "module load cray-python/3.6.1.1; source parsl_env/bin/activate"
    # },
    # 'cooley': {
    #     'username': ALCF_USERNAME,
    #     "account": ALCF_ALLOCATION,
    #     'scheduler_options': "",
    #     "worker_init": "source /home/{}/setup_cooley_env.sh".format(ALCF_USERNAME),
    #     # Once you log onto Cooley, get the ip address of the login machine
    #     # by running >> ip addr show | grep -o 10.236.1.[0-9]*
    #     'public_ip': '10.236.1.193'
    # },
    # },
    # 'ec2': {
    #     "region": "us-east-2",
    #     "image_id": 'ami-82f4dae7',
    #     "key_name": "parsl.test",
    #     # Name of the profile used to identify credentials stored in ~/.aws/config
    #     "profile_name": "parsl",
    # },
    #
    # 'azure': {
    #
    #   # Specifies a username/password which can be used to log into Azure VMs
    #   # These must be specified but are not used by parsl to access the VMs.
    #   'admin_username': 'anyuser',
    #   'password': 'mypassword1234567!',
    #
    #   # Characteristics of the VMs to be started:
    #   'vm_size': 'Standard_D1',
    #   'disk_size_gb': '10',
    #
    #   # Details of the image to be started on each VM.
    #   # Values can be found using, for example, the `az` command line tool:
    #   # az vm image list --publisher Debian
    #   'publisher': 'Debian',
    #   'offer': 'debian-10',
    #   'sku': '10',
    #   'version': 'latest'
    # },
    # 'theta': {
    #     'username': ALCF_USERNAME,
    #     "account": ALCF_ALLOCATION,
    #     'scheduler_options': "",
    #     "worker_init": "source /home/{}/setup_theta_env.sh".format(ALCF_USERNAME),
    #     # Once you log onto theta, get the ip address of the login machine
    #     # by running >> ip addr show | grep -o 10.236.1.[0-9]*
    #     'public_ip': '10.236.1.193'
    # },
    # 'beagle': {
    #     'username': 'fixme',
    #     "script_dir": "fixme",
    #     "scheduler_options": "#SBATCH --constraint=haswell",
    #     "worker_init": """module load python/3.5-anaconda ; source activate parsl_env_3.5"""
    # },
    # 'cc_in2p3': {
    #     'script_dir': "~/parsl_scripts",
    #     'scheduler_options': "",
    #     "worker_init": """export PATH=/pbs/throng/lsst/software/anaconda/anaconda3-5.0.1/bin:$PATH; source activate parsl_env_3.5"""
    # },
    # 'globus': {
    #     'endpoint': 'fixme',
    #     'path': 'fixme',
    #
    #     # remote_writeable should specify a directory on a globus endpoint somewhere else,
    #     # where files can be staged out to via globus during globus staging tests.
    #     # For example:
    #     'remote_writeable': 'globus://af7bda53-6d04-11e5-ba46-22000b92c6ec/home/bzc/'
    # },
    #
}  # type: Dict[str, Any]

# This block attempts to import local_user_opts.py, which
# can provide local overrides to the version-controlled
# user_opts.
# Users can add their own overrides into local_user_opts
# in local_user_opts.py, which should not exist in a
# pristine parsl source tree, and which should help avoid
# accidentally committing secrets and other per-user
# config into version control.
try:
    from .local_user_opts import local_user_opts
    user_opts.update(local_user_opts)

except ImportError:
    pass
