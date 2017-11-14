template_string = '''#!/bin/bash
Universe = vanilla

Executable = $user_script

Output = $submit_script_dir/${jobname}.out.$$(Cluster).$$(Process)
Error = $submit_script_dir/${jobname}.err.$$(Cluster).$$(Process)
Log = $submit_script_dir/${jobname}.$$(Cluster)

leave_in_queue = true

request_cpus = $tasks_per_node

$condor_overrides

queue $nodes
'''

# for later, 
# if we want to remove on preemption, this might work:
#    PERIODIC_REMOVE = (NumJobstarts > 1)
# or if the pilot can trap signals, then we can send a special exit code on
# sigterm/sigkill and remove that way. but then we still need to be careful in
# cases where the worker dies, for example-- no signal is sent
