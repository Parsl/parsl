"""
Specification of user-specific configuration options.

The fields must be configured separately for each user. To disable any associated configurations, comment
out the entry.
"""
user_opts = {
    # 'midway': {
    #     'username': 'fixme',
    #     'script_dir': '/scratch/midway2/fixme/parsl_scripts',
    #     'overrides': 'cd /scratch/midway2/fixme/parsl_scripts; module load Anaconda3/5.0.0.1; source activate parsl_py36; export PARSL_TESTING=True'
    # },
    # 'osg': {
    #     'username': 'fixme',
    #     'script_dir': '/scratch/midway2/fixme/parsl_scripts'
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
    # 'cori': {
    #     'username': 'fixme',
    #     'script_dir': 'fixme',
    #     "overrides": """#SBATCH --constraint=haswell module load python/3.5-anaconda ; source activate parsl_env_3.5"""
    # },
    # 'cooley': {
    #     'username': 'fixme',
    #     "account": 'CSC249ADCD01',
    #     "overrides": "source /home/fixme/setup_cooley_env.sh"
    # },
    # 'swan': {
    #     'username': 'fixme',
    #     'overrides': "module load cray-python/3.6.1.1; source /home/users/fixme/parsl_env/bin/activate"
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
