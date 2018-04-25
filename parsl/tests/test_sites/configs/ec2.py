ec2OnDemand = {
    "sites": [
        {"site": "ec2",
         "auth": {
             "channel": None,
             "profile": "default"
         },
         "execution":
         {"executor": "ipp",
          "provider": "aws",
             "channel": None,
             "block": {
                 "initBlocks": 1,
                 "maxBlocks": 1,
                 "minBlocks": 0,
                 "taskBlocks": 1,
                 "nodes": 1,
                 "walltime": "00:25:00",
                 "options": {
                     "region": "us-east-2",
                     "imageId": 'ami-82f4dae7',
                     "stateFile": "awsproviderstate.json",
                     "keyName": "parsl.test"
                 }
             }
          }
         }
    ]
}

spotNode = {
    "sites": [
        {"site": "ec2",
         "auth": {
             "channel": None,
             "profile": "default"
         },
         "execution":
         {"executor": "ipp",
          "provider": "aws",
             "channel": None,
             "block": {
                 "initBlocks": 1,
                 "maxBlocks": 1,
                 "minBlocks": 0,
                 "taskBlocks": 1,
                 "nodes": 1,
                 "walltime": "00:25:00",
                 "options": {
                     "region": "us-east-2",
                     "imageId": 'ami-82f4dae7',
                     "stateFile": "awsproviderstate.json",
                     "keyName": "parsl.test",
                     "spotMaxBid": 1.0
                 }
             }
          }
         }
    ]
}

badSpotConfig = {
    "sites": [
        {"site": "ec2",
         "auth": {
             "channel": None,
             "profile": "default"
         },
         "execution":
         {"executor": "ipp",
          "provider": "aws",
             "channel": None,
             "block": {
                 "initBlocks": 1,
                 "maxBlocks": 1,
                 "minBlocks": 0,
                 "taskBlocks": 1,
                 "nodes": 1,
                 "walltime": "00:25:00",
                 "options": {
                     "region": "us-east-2",
                     "imageId": 'ami-82f4dae7',
                     "stateFile": "awsproviderstate.json",
                     "keyName": "parsl.test",
                     "spotMaxBid": 0.001  # <--- Price too low
                 }
             }
          }
         }
    ]
}
