template_string = '''#!/bin/bash

#PBS -S /bin/bash
#PBS -N ${jobname}
#PBS -m n
#PBS -k eo
#PBS -l walltime=$walltime
#PBS -o ${submit_script_dir}/${jobname}.submit.stdout
#PBS -e ${submit_script_dir}/${jobname}.submit.stderr
#PBS -v WORKER_LOGGING_LEVEL
${overrides}

export JOBNAME="${jobname}"

${user_script}

'''

#cd / && aprun -n 1 -N 1 -cc none -d 24 -F exclusive /bin/bash -c "/usr/bin/perl /home/yadunandb/.globus/coasters/cscript4670024543168323237.pl http://10.128.0.219:60003,http://127.0.0.2:60003,http://192.5.86.107:60003 0718-5002250-000000 NOLOGGING; echo \$? > /autonfs/home/yadunandb/beagle/run019/scripts/PBS3963957609739419742.submit.exitcode"  1>>/autonfs/home/yadunandb/beagle/run019/scripts/PBS3963957609739419742.submit.stdout 2>>/autonfs/home/yadunandb/beagle/run019/scripts/PBS3963957609739419742.submit.stderr

