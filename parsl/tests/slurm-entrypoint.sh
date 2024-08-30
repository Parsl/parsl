#!/bin/bash
set -e

echo "---> Starting the MUNGE Authentication service (munged) ..."
gosu munge /usr/sbin/munged

echo "---> Starting the slurmctld ..."
exec gosu slurm /usr/sbin/slurmctld -i -Dvvv &

echo "---> Waiting for slurmctld to become active before starting slurmd..."

echo "---> Starting the Slurm Node Daemon (slurmd) ..."
exec /usr/sbin/slurmd -Dvvv &

echo "---> Running user command '${@}'"
exec "$@"
