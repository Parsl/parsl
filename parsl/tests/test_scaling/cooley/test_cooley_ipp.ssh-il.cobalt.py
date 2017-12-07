from parsl import *
import parsl
import libsubmit

print("WARNING: Make sure the username and remote dirs are setup")

config = {
    "sites": [{
        "site": "ALCF_Cooley_SSH_Remote",
        "auth" : {
            "channel" : "ssh-il",
            "hostname" : "cooleylogin1.alcf.anl.gov",
            "username" : "yadunand",
            "scriptDir" : "/home/yadunand/parsl_scripts/"
        },
        "execution":  {
            "executor": "ipp",
            "provider": "cobalt",
            "scriptDir": ".scripts",
            "block": {
                "nodes": 1,
                "walltime": "00:20:00",
                "initBlocks": 1,
                "maxBlocks": 1,
                "minBlocks": 0,
                "taskBlocks": "$CORES",
                "scriptDir": ".",
                "options": {
                    "overrides": "soft add anaconda; source activate /home/yadunand/.conda/envs/parsl_env_3.5"
                },
            },
        },
    }],
    "controller": {"publicIp": "*"},
    "globals": {"lazyErrors": True},
}

dfk = DataFlowKernel(config=config)


@App("python", dfk)
def test():
    import platform
    return "Hello from {0}".format(platform.uname())


results = {}
for i in range(0,5):
    results[i] = test()

print("Waiting ....")
print(results[0].result())
