#!/bin/bash
set -eu

pip install requirements.txt

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
dir_name=${PWD##*/}
package="cloudcoreo-directory-backup"
file="${package}.py"
tar_file="${package}.tar.gz"

## ensure the file is executable
chmod +x "$file"

## get the version
version="$(./$file --version)"

rm -f ./$tar_file
tar -czf $tar_file $file

(
    cd $DIR/../tools
    
    vagrant up
    
    vagrant ssh <<EOF
###################################
### BUILD DIRECTORY BACKUP RPM ####
###################################

# Prepare to create an RPM from the tarball
mkdir -p /tmp/${package}-rpm-buildworking
buildroot=/tmp/${package}-rpm-buildworking

rm -rf /tmp/${package}-rpm-buildworking
mkdir -p /tmp/${package}-rpm-buildworking

echo "Starting build of ${package} RPM..."
/vagrant/tar2rpm.sh /cloudcoreo/$dir_name/${tar_file} --topdir \$buildroot --target /opt --name ${package} --summary "${package}" --version "$version" --release 1 --arch noarch || echo "RPM build failed... see logs"

cp /tmp/${package}-rpm-buildworking/RPMS/noarch/${package}-${version}-1.noarch.rpm /cloudcoreo/$dir_name/

EOF

)
args=""
source ~/.aws-sdk/pallen-dev
aws s3 cp ${package}-${version}-1.noarch.rpm s3://cloudcoreo-yum/inbox/tools/${package}-${version}-1.noarch.rpm $args

rm -f ${package}-${version}-1.noarch.rpm

(
    cd $DIR/../tools
    
    vagrant destroy --force
)
