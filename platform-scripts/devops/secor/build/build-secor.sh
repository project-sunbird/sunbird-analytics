#!/bin/sh

environment=$1 #dev
type=$2        #raw_telem or analytics
tag="$1-$2"

# Copy config to secor_dir/src/main/config
`ansible-playbook -i hosts secor.yaml --tags "$tag" `
