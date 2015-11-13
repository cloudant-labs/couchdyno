from setuptools import setup

setup(
    name="dyno",
    version="0.1.0",
    install_requires=["CouchDB"],
    py_modules=["dyno"],
    entry_points={
        'console_scripts': [
            'dyno-info=dyno:info',
            'dyno-setup=dyno:setup',
            'dyno-execute=dyno:execute',
        ]
    }
)
