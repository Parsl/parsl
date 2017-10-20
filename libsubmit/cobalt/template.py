template_string = '''#!/bin/bash -e

echo "Starting Cobalt job script"

echo "----Cobalt Nodefile: -----"
cat $$COBALT_NODEFILE
echo "--------------------------"

$overrides

$user_script

echo "End of Cobalt job"
'''
