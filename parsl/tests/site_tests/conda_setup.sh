#!/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
echo $SCRIPTPATH
PARSL_ROOT=$(dirname $(dirname $(dirname $SCRIPTPATH)))
PARSL_GITHASH=$(git rev-parse --short HEAD)

CONDA_TARGET=parsl_$PARSL_GITHASH.py3.7
export CONDA_TARGET

if [[ "$CONDA_TARGET" == "$CONDA_DEFAULT_ENV" ]]
then
    echo "Conda target env $CONDA_TARGET loaded"
    exit 0
fi

create_conda() {
    pushd .
    cd $PARSL_ROOT

    if [[ "$(hostname)" =~ .*thetalogin.* ]]
    then
	echo "On theta"
	module load miniconda-3/latest
	conda create -p $CONDA_TARGET --clone $CONDA_PREFIX
	conda activate $CONDA_TARGET
	# Theta is weird, we do explicit install
	pip install -r test-requirements.txt
	yes | conda install pip psutil
	python3 setup.py install
	return
    fi

    yes | conda install pip psutil
    python3 -m pip install .
    pip install -r test-requirements.txt

    popd

}

create_conda
