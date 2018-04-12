# ensure that this script is being sourced
if [ ${BASH_VERSINFO[0]} -gt 2 -a "${BASH_SOURCE[0]}" = "${0}" ] ; then
    echo ERROR: script ${BASH_SOURCE[0]} must be executed as: source ${BASH_SOURCE[0]}
    exit 1
fi

# Env for Midway
export MIDWAY_USERNAME="yadunand"

# Env for OSG
export OSG_USERNAME="yadunand"

# Env for CC_In2P3 French grid
export CC_IN2P3_USERNAME="yadunand"
export CC_IN2P3_HOME="yadunand"

# Test globus
export GLOBUS_ENDPOINT="066539c4-2e0b-11e8-b884-0ac6873fc732"
export GLOBUS_EP_PATH=$PWD/test_globus
