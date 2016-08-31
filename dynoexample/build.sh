#!/bin/sh

set -e

if which pip >/dev/null; then
    echo "Found pip"
else
    echo "Could not find pip. Install python's pip installer then try again"
    exit 1
fi

if which virtualenv >/dev/null; then
    echo "Found virtualenv"
else
    echo "Could not find virtualenv. Install virtualenv then try again"
    exit 1
fi



if [ ! -f ./venv/bin/rep ]; then

    if [ -z "${SAPI_CI_USER+x}" ]; then
        echo "SAPI_CI_USER not set. Using cloudant_sapi_ci"
        SAPI_CI_USER=cloudant_sapi_ci
    else
        echo "Using found SAPI_CI_USER=${SAPI_CI_USER}"
    fi

    if [ -z "${SAPI_CI_PASSWD+x}" ]; then
        echo "SAPI_CI_PASSWD not defined. Trying to get it from knife data bag..."
        SAPI_CI_PASSWD=`knife data bag show -F json cloudant_sapi ci_user 2> /dev/null | python -c "import sys,json; d=json.loads(sys.stdin.read()); print d['web_password']"`
        if [ -z  "${SAPI_CI_PASSWD+x}" ]; then
            echo "ERROR: Could not get SAPI_CI_PASSWD from cloudant_sapi ci_user data bag"
            echo "For development if ~/.clou file is present can set SAPI_CI_USER adn SAPI_CI_PASSWD from web user and password"
            exit 1
        fi
        echo "Got pypi SAPI_CI_PASSWD from knife data bag."
    else
        echo "Using found SAPI_CI_PASSWD"
    fi

    rm -rf venv
    virtualenv venv
    . ./venv/bin/activate
    pip install -q --upgrade pip
    pip install -q -i "https://${SAPI_CI_USER}:${SAPI_CI_PASSWD}@pypi.cloudant.com/simple" -r requirements.txt
else
    . ./venv/bin/activate
fi

rm -rf build dist dyno.egg-info
python setup.py install -q

echo "Install finished"



