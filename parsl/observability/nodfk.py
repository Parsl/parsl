# looks for log lines that are not associated with a DFK
# because pretty much all parsl log lines *should* be
# associated with a DFK - so this could even be a Pytest
# to run...

from parsl.observability.getlogs import getlogs


logs = getlogs()

print(f"imported logs with {len(logs)} entries")

nodfk_logs = [lr for lr in logs if 'parsl_dfk' not in lr]

print(f"no-DFK logs: {len(nodfk_logs)} entries")

import random
random.shuffle(nodfk_logs)

for n in range(0, min(len(nodfk_logs), 10)):
    print(nodfk_logs[n])
