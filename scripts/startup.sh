#!/bin/bash

sudo apt-get update
sudo apt-get install -y git
sudo apt-get install -y default-jre
sudo apt-get install -y default-jdk
sudo apt-get install -y ant
sudo apt-get install -y tmux

git clone https://github.com/jtwarren/simple_cql.git

pushd ~/simple_cql
ant runcqltest -Dtest=SimpleAdTest

export _JAVA_OPTIONS="-Xmx32g -Xms4g"