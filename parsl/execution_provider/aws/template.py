template_string = '''#!/bin/bash
sudo apt-get install -y python3 python3-pip
sudo pip3 install ipyparallel parsl

$user_script
'''
