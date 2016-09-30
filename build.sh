#!/bin/sh

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

if [ ! -f ./venv/bin/couchdyno-execute ]; then
    rm -rf venv
    virtualenv venv
    . ./venv/bin/activate
    pip install -q --upgrade pip
    pip install -q -r requirements.txt
else
    . ./venv/bin/activate
fi

rm -rf build dist couchdyno.egg-info
python setup.py install -q

echo "Install finished"
echo
echo "Can now run: "
echo "    ./venv/bin/couchdyno-{setup|execute|info}"
echo "    ./venv/bin/rep"



