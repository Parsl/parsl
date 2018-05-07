runtime = {
    'midway': {
        'username': 'annawoodard',
        'script_dir': '/scratch/midway2/annawoodard/parsl_scripts',
        'options': {
            'partition':
            'westmere',
            'overrides':
            'cd /scratch/midway2/annawoodard/parsl_scripts; module load Anaconda3/5.0.0.1; source activate parsl_py36; export PARSL_TESTING=True'
        }
    },
    'osg': {
        'username': 'annawoodard',
        'script_dir': '/scratch/midway2/annawoodard/parsl_scripts'
    },
    'ec2': {
        "options": {
            "region": "us-east-2",
            "imageId": 'ami-82f4dae7',
            "stateFile": "awsproviderstate.json",
            "keyName": "parsl.test"  # Update to MATCH
        }
    }
}
