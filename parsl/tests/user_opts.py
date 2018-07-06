"""
Specification of user-specific configuration options.

The fields must be configured separately for each user. To disable any associated configurations, comment
out the entry.
"""

# PUBLIC_IP = "52.86.208.63" # "128.135.250.229"
# MIDWAY_USERNAME = "yadunand"
# OSG_USERNAME = "yadunand"
# SWAN_USERNAME = "p02509"
# CORI_USERNAME = "yadunand"
# ALCF_USERNAME = "yadunand"
# ALCF_ALLOCATION = "CSC249ADCD01"
# COMET_USERNAME = "yadunand"

user_opts = {
    # "public_ip" : PUBLIC_IP,
    # 'comet': {
    #     'username': COMET_USERNAME,
    #     'script_dir': '/home/{}/parsl_scripts'.format(COMET_USERNAME),
    #     'overrides': 'export PATH:/home/{}/anaconda3/bin/:$PATH; source activate parsl_0.5.0_py3.6;'.format(COMET_USERNAME),
    # },
    # 'midway': {
    #     'username': MIDWAY_USERNAME,
    #     'script_dir': '/scratch/midway2/{}/parsl_scripts'.format(MIDWAY_USERNAME),
    #     'overrides': 'cd /scratch/midway2/{}/parsl_scripts; module load Anaconda3/5.1.0; source activate parsl_testing;'.format(MIDWAY_USERNAME),
    # },
    # 'osg': {
    #     'username': OSG_USERNAME,
    #     'script_dir': '/home/{}/parsl_scripts'.format(OSG_USERNAME),
    #     'worker_setup' : 'module load python/3.5.2; python3 -m venv parsl_env; source parsl_env/bin/activate; python3 -m pip install parsl==0.5.2'
    # },
    # 'cori': {
    #     'username': CORI_USERNAME,
    #     'script_dir': "/global/homes/y/{}/parsl_scripts".format(CORI_USERNAME),
    #     "overrides": """#SBATCH --constraint=haswell
    #     module load python/3.6-anaconda-4.4 ;
    #     source activate parsl_env_3.6"""
    # },
    # 'swan': {
    #     'username': SWAN_USERNAME,
    #     'script_dir' : "/home/users/{}/parsl_scripts".format(SWAN_USERNAME),
    #     'overrides': "module load cray-python/3.6.1.1; source parsl_env/bin/activate"
    # },
    # 'cooley': {
    #     'username': ALCF_USERNAME,
    #     "account": ALCF_ALLOCATION,
    #     "overrides": "source /home/{}/setup_cooley_env.sh".format(ALCF_USERNAME),
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
    # 'theta': {
    #     'username': ALCF_USERNAME,
    #     "account": ALCF_ALLOCATION,
    #     "overrides": "source /home/{}/setup_theta_env.sh".format(ALCF_USERNAME),
    #     # Once you log onto theta, get the ip address of the login machine
    #     # by running >> ip addr show | grep -o 10.236.1.[0-9]*
    #     'public_ip': '10.236.1.193'
    # },
    # Options below this line are untested ----------------------------------------------------
    # 'beagle': {
    #     'username': 'fixme',
    #     "script_dir": "fixme",
    #     "overrides": """#SBATCH --constraint=haswell module load python/3.5-anaconda ; source activate parsl_env_3.5"""
    # },
    # 'cc_in2p3': {
    #     'script_dir': "~/parsl_scripts",
    #     "overrides": """export PATH=/pbs/throng/lsst/software/anaconda/anaconda3-5.0.1/bin:$PATH; source activate parsl_env_3.5"""
    # },
    # 'globus': {
    #     'endpoint': 'fixme',
    #     'path': 'fixme'
    # }
}
