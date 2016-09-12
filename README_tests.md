Replication Tests
=================

Replication tests originally started as a list of manually run
[Rep](README_rep.md) experiments, which were then later made into pytest tests.

Common pytest fixtures are located in [conftest.py](tests/conftest.py) module.
That is a special module used by the framework. It contains fixtures and skip
markers.

Tests are run in two major modes:

 1. *Local* cluster: if `--cluster_repo=<dir|giturl>` is  specified. A cluster
 will be started by the test framework and tests will be run against that.
 In addition, some tests are only able to run in this mode, because, for example,
 they need to stop some cluster nodes to tests failure scenarios.

 2. *Remote* cluster: if `--cluster_repo` is not defined then `--server_url` is
 used to connect to the server/account. By default it assume a cluster started on
 localhost with adm:pass admin username and password, but could also specify a
 remote account. The difference from `local` mode is the test framewor will
 not start and stop the cluster.

Local cluster mode assumes control over running and stopping a dev cluster
on local machine. It will stop an existing cluster started from `./dev/run`

Tests in `./tests` folder are alphabetized using a numerical index 00, 01, etc.
That is done in order to run them in order, usually from the most basic ones to
more complex. If `-x` option is used, then tests can be stopped early if basic
tests start failing.


Running Tests
--------------

There is a convenience `test.sh` script provided. It first runs `build.sh` to
make sure dependencies and venv are set up. Then runs `pytest`.  All the
options available to `pytest` can be passed in to `test.sh` script.

Examples
---------

 1. Run all tests. Assumig a default `./dev/run` dev cluster is running,
 node1 listening on port 15984, admin user:pass set to `adm:pass`:
     ```
     ./test.sh
     ```

 1. Same as 1. but stop on first failure:
     ```
     ./test.sh -x
     ```

 1. Use a local cluster, with source located in `~/src/db` folder, and a ramdisk
 temp location to build and start it from
 ```
 ./test.sh -x --cluster_repo="~/src/db" --cluster_tmpdir=/Volumes/ramdisk/tmptest
 ```

 1. Use `http://adm:pass@localhost:15984` as a remote cluster and only run tests
 from basic module, while displaying stdout emitted during tests:
 ```
 ./test.sh tests/test_rep_00_basics.py -v -s
 ```

 1. Use default `server_url` for replication cluster but different source and
 target clusters:
 ```
 ./test.sh --source_url="https://<user>:<pass>@<host>" --target_url="https://<user>:<pass>@<host>"
 ```
