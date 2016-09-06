import pytest
from dyno.cluster import get_cluster
from dyno.cfg import getcfg
from dyno.rep import Rep


@pytest.fixture(scope="session")
def local_cluster():
    cluster = get_cluster()
    print "Setup cluster", cluster
    yield cluster
    print "Clean up cluster", cluster
    if cluster:
        cluster.cleanup()


@pytest.fixture(scope="module")
def running_local_cluster(local_cluster):
    if local_cluster is None:
        print "No local cluster specified"
        yield None
    else:
        print "Starting cluster", local_cluster
        with local_cluster.running() as running_cluster:
            yield running_cluster
        print "Stopped cluster", local_cluster


_rep = None

def get_rep():
    if _rep is None:
        raise Exception("No current replication configuration object is defined")
    return _rep


@pytest.fixture(scope="module")
def rep(running_local_cluster):
    global _rep
    if running_local_cluster:
        _rep = runing_local_cluster.get_rep()
        yield _rep
        _rep = None
    else:
        print "Setting up module rep as default Rep() instance"
        _rep = Rep()
        yield _rep
        print "Resetting module rep"
        _rep = None
