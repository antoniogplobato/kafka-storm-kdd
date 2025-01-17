#!/bin/bash

while getopts t: option; do
    case $option in
        t) TOTAL_NODES=$OPTARG;;
    esac
done

function disableFirewall {
    echo "Disabling the Firewall"
    service iptables save
    service iptables stop
    chkconfig iptables off
}

function writeHostFile {
    echo "setting up /etc/hosts file"

    echo "127.0.0.1   localhost localhost.localdomain localhost4 localhost4.localdomain4" > /etc/hosts
    echo "::1         localhost localhost.localdomain localhost6 localhost6.localdomain6" >> /etc/hosts

    for i in $(seq 1 $TOTAL_NODES); do
        #z = $i - 1
        echo "10.0.0.10${i}   node${i} zkpr${i-1}" >> /etc/hosts
    done
}

function installDependencies {
    echo "Installing Supervisor"
    yum install -y epel-release
    yum install -y python-pip unzip

    pip install --upgrade pip

    #yum installs an old version of setuptools and iniparse
    pip install -U setuptools
    pip install -U iniparse

    pip install supervisor
    pip install argparse

    cp /vagrant/resources/supervisord.conf /etc/supervisord.conf
    cp /vagrant/resources/upstart-supervisor.conf /etc/init/supervisor.conf

    echo "CREATING TEMPORARY FOLDER"
    mkdir -p "/vagrant/resources/tmp"
    mkdir -p /etc/supervisor.d
    mkdir -p /var/log/supervisor

    yum install -y wget
    pip install kafka-python
    pip install scapy
    pip install kazoo
    pip install git+https://github.com/tpiscitell/kurator.git#egg=kurator-master
}

function installNtpd {
    yum install -y ntp

    ntpdate 0.pool.ntp.org

    service ntpd start
    chckconfig ntpd on
}

function configureUlimit {
    echo "root  hard    nofile  10240" > /etc/security/limits.d/50-root.conf
}

configureUlimit
disableFirewall
writeHostFile
installDependencies
