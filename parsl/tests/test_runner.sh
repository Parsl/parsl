#!/bin/bash
#set -e
echo "##############################################################"
echo "#### Begin tests "
python3 -c "import parsl;     print('Parsl version     :', parsl.__version__)"
python3 -c "import libsubmit; print('Libsubmit version :', libsubmit.__version__)"
echo "##############################################################"

if [[ ! -e "user_env.sh" ]]
then
    echo "WARNING: user_env.sh script missing"
    echo "ERROR: Cannot proceed"
    exit -1
else
    echo "Loading user_env.sh script"
    source user_env.sh
fi


###### Defaults ############################################
echo "Setting to test mode"
export EXIT_EARLY=true
export PARSL_TESTING=true

############################################################

wrapper() {
    echo "#############################################"
    echo "Running $1"
    echo "============================================="
    if [[ "$EXIT_EARLY" == "true" ]]
    then
        opts="-x"
    fi
    pushd .
    $1 $opts
    popd
    echo "Done $1"
    echo "============================================="
}

run_threads() {
    echo "Running Threads"
    nosetests -v $1 test_threads
}

run_threads() {
    nosetests -v $1 test_threads
}

run_error_handling() {
    nosetests -v $1 test_error_handling
}

run_checkpointing() {
    for test in test_checkpointing/test*; do
        nosetests -v $1 $test || exit -2;
    done;
}
run_data() {
    nosetests -v $1 test_data
}

run_stress() {
    cd test_stress
    nosetests -v $1 .
}

run_globus() {
    cd test_globus
    nosetests -v $1 .
}

run_flowcontrol() {
    cd test_flowcontrol
    for t in test*py
    do
        echo "Running $test"
        python3 $t || exit -2
    done
}

run_containers() {
    echo "This is not standardized"
    export PATH="/home/yadu/miniconda3/bin:$PATH"
    source activate parsl_py3.6.4
    cd test_docker
    nosetests -v $1 test_docker*
}

run_ipp() {
    cd test_ipp
    failed=0
    # We run each test one at a time to ensure DFK cleanup
    for test in test*py;
    do
        echo "=====Running test : $test =================="
        nosetests -v $1 $test;
        if [[ $? != 0 ]]; then failed=$(($failed+1)) ;fi
        killall -u $USER ipcontroller -9
        killall -u $USER ipengine -9
    done;

}



test_standard () {
    wrapper run_threads
    wrapper run_data
    wrapper run_checkpointing
    wrapper run_error_handling
    ./cleanup
}


test_provisional() {
    #wrapper run_containers
    #wrapper run_stress
    #wrapper run_globus
    #wrapper run_flowcontrol
    wrapper run_ipp
}

#test_standard
test_provisional
#wrapper run_ipp
#wrapper run_globus

