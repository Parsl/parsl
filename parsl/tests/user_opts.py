"""
Specification of user-specific configuration options.

The fields must be configured separately for each user. To disable any associated configurations, comment
out the entry.
"""

user_opts = {
    #"public_ip" : PUBLIC_IP,
    #'midway': {
    #    'username': MIDWAY_USERNAME,
    #    'script_dir': '/scratch/midway2/{}/parsl_scripts'.format(MIDWAY_USERNAME),
    #    'overrides': 'cd /scratch/midway2/{}/parsl_scripts; module load Anaconda3/5.1.0; source activate parsl_testing;'.format(MIDWAY_USERNAME),
    #},
    #'osg': {
    #    'username': OSG_USERNAME,
    #    'script_dir': '/home/{}/parsl_scripts'.format(OSG_USERNAME),
    #    'worker_setup' : 'module load python/3.5.2; python3 -m venv parsl_env; source parsl_env/bin/activate; python3 -m pip install parsl==0.5.2'
    #},
    #'cori': {
    #    'username': CORI_USERNAME,
    #    'script_dir': "/global/homes/y/{}/parsl_scripts".format(CORI_USERNAME),
    #    "overrides": """#SBATCH --constraint=haswell
    #    module load python/3.6-anaconda-4.4 ;
    #    source activate parsl_env_3.6"""
    #},
    #'swan': {
    #    'username': SWAN_USERNAME,
    #    'script_dir' : "/home/users/{}/parsl_scripts".format(SWAN_USERNAME),
    #    'overrides': "module load cray-python/3.6.1.1; source parsl_env/bin/activate"
    #},
    # 'cooley': {
    #     'username': 'fixme',
    #     "account": 'CSC249ADCD01',
    #     "overrides": "source /home/fixme/setup_cooley_env.sh"
    # },
    # 'ec2': {
    #     "region": "us-east-2",
    #     "image_id": 'ami-82f4dae7',
    #     "key_name": "parsl.test"
    # },
    # 'beagle': {
    #     'username': 'fixme',
    #     "script_dir": "fixme",
    #     "overrides": """#SBATCH --constraint=haswell module load python/3.5-anaconda ; source activate parsl_env_3.5"""
    # },
    # 'theta': {
    #     'username': 'fixme',
    #     "script_dir": "/home/fixme/parsl_scripts/",
    #     "account": "CSC249ADCD01",
    #     "overrides": "export PATH=/home/fixme/theta_parsl/anaconda3/bin/:$PATH; source activate /home/fixme/theta_parsl/anaconda3/envs/theta_parslenv",
    #     # Once you log onto theta, get the ip address of the login machine
    #     # by running >> ip addr show | grep -o 10.236.1.[0-9]*
    #     'public_ip': '10.236.1.193'
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
