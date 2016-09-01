from setuptools import setup

setup(
    name="dyno",
    version="0.5.0",
    url="https://github.com/cloudant/dyno",
    author="Nick Vatamaniuc",
    author_email="nvatama@us.ibm.com",
    install_requires=["CouchDB","ipython", "ConfigArgParse", "pytest"],
    packages = ["dyno"],
    entry_points={
        'console_scripts': [
            'dyno-info=dyno:info',
            'dyno-setup=dyno:setup',
            'dyno-execute=dyno:execute',
            'rep=rep:_interactive',
        ]
    }
)
