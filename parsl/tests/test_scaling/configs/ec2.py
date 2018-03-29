"""Config for EC2.

Block {Min:0, init:1, Max:1}
==================
| ++++++++++++++ |
| |    Node    | |
| |            | |
| | Task  Task | |
| |            | |
| ++++++++++++++ |
==================

"""
singleNode = {
    "sites": [
        {"site": "Remote_IPP",
         "auth": {
             "channel": None,
             "profile": "default",
         },
         "execution": {
             "executor": "ipp",
             "provider": "aws",
             "block": {  # Definition of a block
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 2,       # total tasks in a block
                 "walltime": "01:00:00",
                 "initBlocks": 1,
                 "options": {
                     "region": "us-east-2",
                     "imageId": 'ami-82f4dae7',
                     "stateFile": "awsproviderstate.json",
                     "keyName": "parsl.test"  # Update to MATCH
                 }
             }
         }
         }
    ],
    "globals": {"lazyErrors": True},
    "controller": {"publicIp": '*'}
}


"""
Block {Min:0, init:1, Max:1}
==================
| ++++++++++++++ |
| |    Node    | |
| |            | |
| | Task  Task | |
| |            | |
| ++++++++++++++ |
==================

"""
spotNode = {
    "sites": [
        {"site": "Remote_IPP",
         "auth": {
             "channel": None,
             "profile": "default",
         },
         "execution": {
             "executor": "ipp",
             "provider": "aws",
             "block": {  # Definition of a block
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 2,       # total tasks in a block
                 "walltime": "01:00:00",
                 "initBlocks": 1,
                 "options": {
                     "region": "us-east-2",
                     "imageId": 'ami-82f4dae7',
                     "stateFile": "awsproviderstate.json",
                     "keyName": "parsl.test",
                     "spotMaxBid": 2.0  # MUST SET SPOT PRICE TO MATCH
                 }
             }
         }
         }
    ],
    "globals": {"lazyErrors": True},
    "controller": {"publicIp": '*'}
}


"""
Block {Min:0, init:1, Max:1}
==================
| ++++++++++++++ |
| |    Node    | |
| |            | |
| | Task  Task | |
| |            | |
| ++++++++++++++ |
==================

"""
badSpotConfig = {
    "sites": [
        {"site": "Remote_IPP",
         "auth": {
             "channel": None,
             "profile": "default",
         },
         "execution": {
             "executor": "ipp",
             "provider": "aws",  # LIKELY SHOULD BE BOUND TO SITE
             "block": {  # Definition of a block
                 "nodes": 1,            # of nodes in that block
                 "taskBlocks": 2,       # total tasks in a block
                 "walltime": "01:00:00",
                 "initBlocks": 1,
                 "options": {
                     "instanceType": "m4.4xlarge",
                     "region": "us-east-2",
                     "imageId": 'ami-82f4dae7',
                     "stateFile": "awsproviderstate.json",
                     "keyName": "parsl.test",
                     "spotMaxBid": 0.001,
                 }
             }
         }
         }
    ],
    "globals": {"lazyErrors": True},
    "controller": {"publicIp": '*'}
}
