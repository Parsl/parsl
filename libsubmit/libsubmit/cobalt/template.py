template_string = '''#!/bin/bash

echo "Starting Cobalt job script"

echo "----Cobalt Nodefile: -----"
cat $$COBALT_NODEFILE
echo "--------------------------"
echo "--------ENV VARS----------"
env
echo "--------------------------"

$cobalt_overrides

$user_script

echo "End of Cobalt job"
'''
