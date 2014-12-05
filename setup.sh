#!/bin/bash

# Install required packages
sudo apt-get update
sudo apt-get -y install git
sudo apt-get -y install g++ autoconf automake make
sudo apt-get -y install libxt-dev
sudo apt-get -y install x11proto-print-dev
sudo apt-get -y install libxpm-dev
sudo apt-get -y install libXext-dev
sudo apt-get -y install patch
sudo apt-get -y install libxmu-dev

# Install postgresql packages
sudo apt-get -y install postgresql-9.3
sudo apt-get -y install libpq-dev
sudo apt-get -y install postgres-xc-client
sudo apt-get -y install postgres-xc

perl -MCPAN -e 'install DBI'
perl -MCPAN -e 'install DBD::Pg'
sudo cpan DBD::PgPP

mkdir ~/linear_data

if [ ! -d "~/MITSIMLab" ]; then
  mkdir -p ~/MITSIMLab
  pushd ~/MITSIMLab
  wget http://www.cs.brandeis.edu/~linearroad/files/mitsim.tar.gz
  tar zxvf mitsim.tar.gz
  popd
fi


initdb --username=linear --nodename=node --pgdata=db5
createdb --no-password

# git clone https://github.com/jtwarren/simple_cql.git

