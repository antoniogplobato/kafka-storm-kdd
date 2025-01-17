#!/bin/bash

JRE_RPM=jre-7u79-linux-x64.rpm
ZOOKEEPER_VERSION=zookeeper-3.4.6
KAFKA_SCALA_VERSION=2.10
KAFKA_VERSION_NUM=0.8.2.1
KAFKA_VERSION="kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION_NUM}"
STORM_VERSION=apache-storm-0.9.4

# So we dont need to pass in i to the scripts
NODE_NUMBER=`hostname | tr -d node`


function downloadFile {

    url="${1}"
    filename="${2}"

    cached_file="/vagrant/resources/tmp/${filename}"

    if [ ! -e $cached_file ]; then
        echo "Downloading ${filename} from ${url} to ${cached_file}"
        echo "This will take some time. Please be patient..."
        wget -nv -O $cached_file $url
    fi

    TARBALL=$cached_file
}

function downloadApacheFile {

    project="${1}"
    version="${2}"
    filename="${3}"

    closest_url=`python /vagrant/scripts/closest-mirror.py ${project} -v ${version} -f ${filename}`

    downloadFile $closest_url $filename
}

function join {
    local IFS="$1"; shift; echo "$*"
}

function generateZkString {
    # Yes its ugly, but so is bash :)
    ZK_STRING=`python -c "print ','.join([ 'node{0}:2181'.format(x) for x in range(2,${1}+1)])"`
}

function generateZkStringNoPorts {
    ZK_STRING_NOPORTS=`python -c "print ','.join([ 'node{0}'.format(x) for x in range(2,${1}+1)])"`
}

function safeSymLink {
    target=$1
    symlink=$2

    if [ -e $symlink ]; then
        echo "${symlink} exists. Deleting."
        rm $symlink
    fi

    ln -s $target $symlink
}

function commentLine {
    line="${1}"
    file="${2}"

    echo "Commenting out '${line}' from ${file}"
    sed -i "s/^${line}/# ${line}/" $file
}
