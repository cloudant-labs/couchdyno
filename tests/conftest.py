import pytest
from dyno.cluster import get_cluster
from dyno.cfg import unused_args, getcfg
from dyno.rep import Rep


def pytest_cmdline_preparse(args):
    args[:] = unused_args()


def is_local():
    cfg = getcfg()
    return bool(cfg.cluster_repo)


skip_if_not_local = pytest.mark.skipif(not is_local(), reason="Not a local cluster")

@pytest.fixture(scope="session")
def session_cfg():
    return getcfg()


@pytest.fixture(scope="session")
def local_cluster(session_cfg):
    cluster = get_cluster(cfg=session_cfg)
    if cluster:
        print "\n * Setup cluster", cluster
    yield cluster
    if cluster:
        print "\n * Clean up cluster", cluster
    if cluster:
        cluster.cleanup()


@pytest.fixture(scope="module")
def running_local_cluster(local_cluster):
    if local_cluster is None:
        print "\n * No local cluster specified"
        yield None
    else:
        print "\n * Starting cluster", local_cluster
        with local_cluster.running() as running_cluster:
            yield running_cluster
        print "\n * Stopped cluster", local_cluster


_rep = None
_running_cluster = None

def get_rep():
    if _rep is None:
        raise Exception("No current replication configuration object is defined")
    return _rep

def get_running_cluster():
    if _running_cluster is None:
        raise Exception("No current running cluster")
    return _running_cluster


@pytest.fixture(scope="module")
def rep(session_cfg, running_local_cluster):
    global _rep, _running_cluster
    if running_local_cluster:
        _running_cluster = running_local_cluster
        _rep = running_local_cluster.get_rep()
        yield _rep
        _rep = None
    else:
        print "\n * Setting up module rep as default Rep() instance"
        _running_cluster = running_local_cluster
        _rep = Rep(cfg=session_cfg)
        yield _rep
        print "\n * Resetting module rep"
        _rep = None
