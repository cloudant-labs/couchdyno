from setuptools import setup

setup(
    name="couchdyno",
    version="0.7.0",
    url="https://github.com/cloudant-labs/couchdyno",
    author="Nick Vatamaniuc",
    author_email="nvatama@us.ibm.com",
    install_requires=["CouchDB","ipython", "ConfigArgParse", "pytest"],
    packages = ["couchdyno"],
    entry_points={
        'console_scripts': [
            'couchdyno-info=couchdyno.couchdyno:info',
            'couchdyno-setup=couchdyno.couchdyno:setup',
            'couchdyno-execute=couchdyno.couchdyno:execute',
            'rep=couchdyno.rep:_interactive',
        ]
    }
)
