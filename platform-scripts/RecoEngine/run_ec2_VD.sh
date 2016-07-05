#!/bin/bash
set -e
cwd=`pwd`


scriptDir=/Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/RecoEngine
keyDir=/Users/soma/Documents/ekStep/LP

# move scripts files
scp -i $keyDir/learning-platform.pem $scriptDir/*.* ec2-user@54.169.221.98:/home/ec2-user/scripts/RecoEngine

# move model files
# add later

# execute the script remotely
# ssh -i $keyDir/learning-platform.pem ec2-user@54.169.221.98 'bash -s' < scriptDir$dp_script_ec2_VD.sh
