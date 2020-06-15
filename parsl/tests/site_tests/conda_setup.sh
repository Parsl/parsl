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
        conda create -p $CONDA_TARGET --clone $CONDA_PREFIX --yes --force
        conda activate $CONDA_TARGET
        # Theta is weird, we do explicit install
        pip install -r test-requirements.txt
        conda install pip psutil --yes
        python3 setup.py install
        echo "module load miniconda-3/latest;"           >  ~/setup_parsl_test_env.sh
        echo "conda activate $PWD/$CONDA_TARGET"         >> ~/setup_parsl_test_env.sh
        return

    elif [[ "$(hostname)" =~ .*frontera.* ]]
    then
        echo "On Frontera"
        if [[ -d ~/anaconda3 ]]
        then
            echo "Loading anaconda3 from ~/anaconda3"
            source ~/anaconda3/bin/activate
        else
            echo "Please install conda to your home dir at ~/anaconda3"
        fi
        conda create -p $CONDA_TARGET python=3.7 --yes --force
        conda activate $CONDA_TARGET
        echo "source ~/anaconda3/bin/activate;"          >  ~/setup_parsl_test_env.sh
        echo "conda activate $PWD/$CONDA_TARGET"         >> ~/setup_parsl_test_env.sh

    elif [[ "$(hostname -f)" =~ .*summit.* ]]
    then
        echo "On Summit"
        module load ibm-wml-ce
        conda create -p $CONDA_TARGET --yes --force
        conda activate $PWD/$CONDA_TARGET
        conda install paramiko>=2.7.1 pip numpy psutil pandas --yes
        # conda install --file requirements.txt --yes
        echo "module load ibm-wml-ce"                    >  ~/setup_parsl_test_env.sh
        echo "conda activate $PWD/$CONDA_TARGET"         >> ~/setup_parsl_test_env.sh

    elif [[ "$(hostname)" =~ .*cori.* ]]
    then
        echo "On Cori"
        module load python/3.7-anaconda-2019.07
        conda create -p $CONDA_TARGET python=3.7 --yes --force
        conda activate $PWD/$CONDA_TARGET
        echo "module load python/3.7-anaconda-2019.07;"  >  ~/setup_parsl_test_env.sh
        echo "conda activate $PWD/$CONDA_TARGET"         >> ~/setup_parsl_test_env.sh

    elif [[ "$(hostname)" =~ .*stampede2.* ]]
    then
        echo "On Stampede2"
        if [[ -d ~/anaconda3 ]]
        then
            echo "Loading anaconda3 from ~/anaconda3"
            source ~/anaconda3/bin/activate
        else
            echo "Please install conda to your home dir at ~/anaconda3"
        fi
        conda create -p $CONDA_TARGET python=3.7 --yes --force
        conda activate $PWD/$CONDA_TARGET
        echo "source ~/anaconda3/bin/activate;"          >  ~/setup_parsl_test_env.sh
        echo "conda activate $PWD/$CONDA_TARGET"         >> ~/setup_parsl_test_env.sh

    elif [[ "$(hostname)" =~ .*comet.* ]]
    then
        echo "On Comet"
        if [[ -d ~/anaconda3 ]]
        then
            echo "Loading anaconda3 from ~/anaconda3"
            source ~/anaconda3/bin/activate
        else
            echo "Please install conda to your home dir at ~/anaconda3"
        fi
        conda create -p $CONDA_TARGET python=3.7 --yes --force
        conda activate $PWD/$CONDA_TARGET
        echo "source ~/anaconda3/bin/activate;"          >  ~/setup_parsl_test_env.sh
        echo "conda activate $PWD/$CONDA_TARGET"         >> ~/setup_parsl_test_env.sh

    elif [[ "$(hostname)" =~ .*midway.* ]]
    then
        echo "On Midway"
        module unload python
        module load Anaconda3/5.3.0
        conda create -p $CONDA_TARGET python=3.7 --yes --force
        source /software/Anaconda3-5.3.0-el7-x86_64/bin/activate
        conda activate $PWD/$CONDA_TARGET
        echo "module unload python"                                         >  ~/setup_parsl_test_env.sh
        echo "module load Anaconda3/5.3.0"                                  >> ~/setup_parsl_test_env.sh
        echo "source /software/Anaconda3-5.3.0-el7-x86_64/bin/activate"     >> ~/setup_parsl_test_env.sh
        echo "conda activate $PWD/$CONDA_TARGET"                            >> ~/setup_parsl_test_env.sh

    elif [[ "$(hostname -f)" =~ .*in2p3.* ]]
    then
        echo "On CC-IN2P3"
	export PATH=/pbs/throng/lsst/software/anaconda/anaconda3-5.0.1/bin:$PATH
        conda create -p $CONDA_TARGET python=3.7 --yes --force
        source activate $PWD/$CONDA_TARGET
	conda install pip --yes
	cat <<'EOF' > ~/setup_parsl_test_env.sh
export PATH=/pbs/throng/lsst/software/anaconda/anaconda3-5.0.1/bin:$PATH
export SHARED_FS_OPTIONS='-k "not issue363"'
EOF
        echo "source activate $PWD/$CONDA_TARGET"            >> ~/setup_parsl_test_env.sh

    elif [[ "$(hostname)" =~ .*cooley.* ]]
    then
        echo "On Cooley, testing Kubernetes on PetrelKube"
        if [[ -d ~/anaconda3 ]]
        then
            echo "Loading anaconda3 from ~/anaconda3"
            source ~/anaconda3/bin/activate
        else
            echo "Please install conda to your home dir at ~/anaconda3"
        fi
        conda create -p $CONDA_TARGET python=3.7 --yes --force
        conda activate $PWD/$CONDA_TARGET
        pip install kubernetes
        echo "pip install git+https://github.com/Parsl/parsl@$PARSL_GITHASH" >  ~/setup_parsl_test_env.sh
        echo 'pip install pytest==4.6'                                       >> ~/setup_parsl_test_env.sh

    elif [[ "$(hostname -f)" =~ .*h2ologin.* ]]
    then
        echo "On Blue Waters"
        module load bwpy
        python_version=3.6
        export EPYTHON="python$python_version"
        mkdir -p parsl_$PARSL_GITHASH.py$python_version
        virtualenv --system-site-packages $PWD/parsl_$PARSL_GITHASH.py$python_version
        source $PWD/parsl_$PARSL_GITHASH.py$python_version/bin/activate
        pip install --upgrade pip
        echo "module load bwpy;"                                                         >  ~/setup_parsl_test_env.sh
        echo "cd $PWD;"                                                                  >> ~/setup_parsl_test_env.sh
        echo "source $PWD/parsl_$PARSL_GITHASH.py$python_version/bin/activate;"          >> ~/setup_parsl_test_env.sh

    else
        echo "Unknown site"
        exit -1
    fi

    echo "Installing parsl from $PARSL_ROOT"
    python3 -m pip install .
    python3 -m pip install -r test-requirements.txt

    popd

}

create_conda
