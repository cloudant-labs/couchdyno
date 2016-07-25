from setuptools import setup

setup(
    name="dyno",
    version="0.2.0",
    install_requires=["CouchDB","ipython"],
    py_modules=["dyno","rep"],
    entry_points={
        'console_scripts': [
            'dyno-info=dyno:info',
            'dyno-setup=dyno:setup',
            'dyno-execute=dyno:execute',
            'rep=rep:_interactive',
        ]
    }
)
