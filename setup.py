from setuptools import setup

setup(
    name="dyno",
    version="0.6.0",
    url="https://github.com/cloudant/dyno",
    author="Nick Vatamaniuc",
    author_email="nvatama@us.ibm.com",
    install_requires=["CouchDB","ipython", "ConfigArgParse", "pytest"],
    packages = ["dyno"],
    entry_points={
        'console_scripts': [
            'dyno-info=dyno.dyno:info',
            'dyno-setup=dyno.dyno:setup',
            'dyno-execute=dyno.dyno:execute',
            'rep=dyno.rep:_interactive',
        ]
    }
)
