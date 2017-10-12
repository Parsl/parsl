template_string = '''#!/bin/bash

echo "Starting Cobalt job script"

echo "----Cobalt Nodefile: -----"
cat $$COBALT_NODEFILE
echo "--------------------------"
echo "--------ENV VARS----------"
env
echo "--------------------------"

$overrides

$user_script

echo "End of Cobalt job"
'''
