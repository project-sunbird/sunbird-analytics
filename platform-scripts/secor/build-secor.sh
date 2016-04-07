#!/usr/bin/env bash

environment=$1
type=$2
secor_dir=$3
target_dir=$4

mkdir -p $target_dir
# Copy config to secor_dir/src/main/config
cp -R $environment/$type/*.properties $secor_dir/src/main/config

cd $secor_dir
mvn package
tar -zxvf target/secor-0.2-SNAPSHOT-bin.tar.gz -C $target_dir