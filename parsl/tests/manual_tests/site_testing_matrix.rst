Site Testing
============

Sites that we will test in this exercise:

* Local IPP
* Midway RCC at UChicago
* Open Science Grid (OSG)
* Swan at Cray
* Cori at NERSC
* Cooley (ALCF)
* Theta (ALCF)
* CC_in2p3 French grid
* AWS EC2
* Comet (SDSC)

Test Progress
=============

+------------------------+------------+------------+------------+------------+
|Site                    |Channel     |Provider    |Launcher    |Test Status |
+========================+============+============+============+============+
| Local                  |Local       |Local       |SingleNode  |  PASSED    |
+------------------------+------------+------------+------------+------------+
| Midway (RCC)           |Ssh         |Slurm       |SingleNode  |  PASSED    |
+------------------------+------------+------------+------------+------------+
| Midway (RCC)           |Ssh         |Slurm       |Srun        | PASSED     |
+------------------------+------------+------------+------------+------------+
| Open Science Grid(OSG) |Ssh         |Condor      |SingleNode  | PASSED     |
+------------------------+------------+------------+------------+------------+
| Swan (Cray)            |Ssh         |Torque      |Aprun       | PASSED     |
+------------------------+------------+------------+------------+------------+
| Cori (NERSC)           |Ssh         |Slurm       |SingleNode  | PASSED     |
+------------------------+------------+------------+------------+------------+
| Cori (NERSC)           |Ssh         |Slurm       |Srun        | PASSED     |
+------------------------+------------+------------+------------+------------+
| Cooley (ALCF)          |SshIL       |Cobalt      |SingleNode  | PASSED     |
+------------------------+------------+------------+------------+------------+
| Theta (ALCF)           |Local       |Cobalt      |Aprun       | PASSED     |
+------------------------+------------+------------+------------+------------+
| CC_IN2P3 (French grid) |Local       |GridEngine  |SingleNode  | PASSED     |
+------------------------+------------+------------+------------+------------+
| AWS EC2                |None        |AWS         |SingleNode  | PASSED     |
+------------------------+------------+------------+------------+------------+
| AWS EC2 (spot)         |None        |AWS         |SingleNode  | PASSED     |
+------------------------+------------+------------+------------+------------+
| Azure                  |None        |Azure       |SingleNode  | DEFERRED   |
+------------------------+------------+------------+------------+------------+
| Nova/Jetstream         |None        |Jetstream   |SingleNode  | DEFERRED   |
+------------------------+------------+------------+------------+------------+
| Google Cloud           |None        |GoogleCloud |SingleNode  | DEFERRED   |
+------------------------+------------+------------+------------+------------+
| Comet SDSC             |Ssh         |Slurm       |Srun        | PASSED     |
+------------------------+------------+------------+------------+------------+
