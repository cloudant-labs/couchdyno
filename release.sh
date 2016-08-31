#!/bin/sh
set -e
echo "Building..."
./build.sh > /dev/null
echo "Finished building."
echo
echo "Packaging release..."
python setup.py sdist --formats=gztar > /dev/null
echo "Finsihed packaging."
echo
FILES=`ls dist/dyno-*.tar.gz`
read -p "Upload $FILES ?  " -n 1
[[ ! $REPLY =~ ^[Yy]$ ]] && exit 1
echo
echo "Uploading..."
scp dist/dyno-*.tar.gz pypi.cloudant.com:
echo "Finished uploading."
echo
echo "Updating packages directory..."
ssh pypi.cloudant.com 'sudo mv dyno-*.tar.gz /opt/pypi-server/packages/'
echo "Updated packages directory."
echo "Done."





