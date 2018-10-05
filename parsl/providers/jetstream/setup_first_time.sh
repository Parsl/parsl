

check_tools() {

    which nova
    if [[ $? != 0 ]]
    then
        echo "nova is missing. Try apt-get install nova"
    fi

}

setup_secgroups() {
    echo "Setting up sec groups"
    nova secgroup-create global-ssh "ssh & icmp enabled"
    nova secgroup-add-rule global-ssh tcp 22 22 0.0.0.0/0
    nova secgroup-add-rule global-ssh icmp -1 -1 0.0.0.0/0

}

setup_keypair() {
    ssh-keygen -b 2048 -t rsa -f ${OS_PROJECT_NAME}-api-key -P ""
    nova keypair-add --pub-key ${OS_PROJECT_NAME}-api-key.pub ${OS_PROJECT_NAME}-api-key
}

setup_network() {

    parsl_net="PARSL-priv-net"
    neutron net-create $parsl_net
    neutron net-list
    neutron subnet-create $parsl_net 10.0.0.0/24 --name parsl-api-subnet1
    neutron net-list
    neutron router-create parsl-api-router
    neutron router-interface-add parsl-api-router parsl-api-subnet1
    neutron router-gateway-set parsl-api-router public
    neutron router-show parsl-api-router
}

check_tools
#setup_secgroups
#setup_keypair
setup_network
nova boot parsl-executor-001 --flavor m1.small --image 87e08a17-eae2-4ce4-9051-c561d9a54bde --key-name TG-MCB090174-api-key --security-groups global-ssh --nic net-name=PARSL-priv-net
