#!/bin/bash

sudo apt-get update
sudo apt-get -y install git
sudo apt-get -y install g++ autoconf automake make
sudo apt-get -y install libxt-dev
sudo apt-get -y install x11proto-print-dev
sudo apt-get -y install libxpm-dev
sudo apt-get -y install libXext-dev
sudo apt-get -y install patch
sudo apt-get -y install libxmu-dev

if [ ! -d "~/MITSIMLab" ]; then
  mkdir -p ~/MITSIMLab
  pushd ~/MITSIMLab
  wget http://www.cs.brandeis.edu/~linearroad/files/mitsim.tar.gz
  tar zxvf mitsim.tar.gz
  popd
fi



# git clone https://github.com/jtwarren/simple_cql.git

