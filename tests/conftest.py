import time
import pytest
import couchdb
from dyno.cluster import get_cluster
from dyno.cfg import unused_args, getcfg
from dyno.rep import Rep


QUIET_PERIOD = 2
START_PERIOD = 1


QUICK_REPLICATION = [
    'replicator.start_delay=1',
    'replicator.start_splay=1',
    'replicator.cluster_quiet_period=%d' % QUIET_PERIOD,
    'replicator.cluster_start_period=%d' % START_PERIOD,
]


TIGHT_SCHEDULER = [
    'replicator.max_jobs=2',
    'replicator.max_churn=2',
    'replicator.interval=7000'
]


def pytest_cmdline_preparse(args):
    args[:] = unused_args()


def is_local():
    cfg = getcfg()
    return bool(cfg.cluster_repo)


skip_if_not_local = pytest.mark.skipif(not is_local(),
                                       reason="Not a local cluster")


def has_replicator_scheduler(rep):
    srv = rep.repsrv
    try:
        (code, msg, response) = srv.resource('_scheduler').get_json('jobs')
        if code == 200:
            return True
    except couchdb.ServerError:
        return False
    except couchdb.ResourceNotFound:
        return False


def skip_if_no_replication_scheduler(rep):
    if not has_replicator_scheduler(rep):
        pytest.skip("No replication scheduler found")


def skip_if_replication_scheduler(rep):
    if has_replicator_scheduler(rep):
        pytest.skip("Replication scheduler")


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
        with local_cluster.running(QUICK_REPLICATION) as running_cluster:
            time.sleep(START_PERIOD + 1)
            yield running_cluster
        print "\n * Stopped cluster", local_cluster


@pytest.fixture(scope="module")
def running_local_cluster_with_tight_scheduler(local_cluster):
    if local_cluster is None:
        print "\n * No local cluster specified"
        yield None
    else:
        print "\n * Starting cluster", local_cluster
        settings = QUICK_REPLICATION + TIGHT_SCHEDULER
        with local_cluster.running(settings) as running_cluster:
            time.sleep(START_PERIOD + 1)
            yield running_cluster
        print "\n * Stopped cluster", local_cluster


_rep = None
_running_cluster = None


def get_rep():
    if _rep is None:
        raise Exception("No current replication config object is defined")
    return _rep


def get_running_cluster():
    if _running_cluster is None:
        raise Exception("No current running cluster")
    return _running_cluster


@pytest.fixture(scope="module")
def rep(session_cfg, running_local_cluster_with_tight_scheduler):
    running_cluster = running_local_cluster_with_tight_scheduler
    global _rep, _running_cluster
    if running_cluster:
        _running_cluster = running_cluster
        _rep = running_cluster.get_rep()
        yield _rep
        _rep.clean()
        _rep = None
    else:
        print "\n * Setting up module rep as default Rep() instance"
        _running_cluster = running_cluster
        _rep = Rep(cfg=session_cfg)
        yield _rep
        _rep.clean()
        print "\n * Resetting module rep"
        _rep = None
