import time
import pytest
import conftest


QUIET_PERIOD = 2
START_PERIOD = 1

# local_cluster is a common fixture from conftest.py

@pytest.fixture
def cluster_with_short_quiet_time(local_cluster):
    settings = [
        'replicator.start_delay=1',
        'replicator.start_splay=1',
        'replicator.cluster_quiet_period=%d' % QUIET_PERIOD,
        'replicator.cluster_start_period=%d' % START_PERIOD
    ]
    with local_cluster.running(settings) as running_cluster:
            time.sleep(START_PERIOD+1)
            yield running_cluster


@conftest.skip_if_not_local
def test_migration_on_node_failure(cluster_with_short_quiet_time):
    """
    Using a cluster with a short replicator cluster quiet period
    (to avoid waiting for too long):
       - create 1-to-n replications
       - fill source db 100 documents
       - after a few cycles kill node 2
       - verify all changes are still replicating
    """
    cluster = cluster_with_short_quiet_time
    rep = cluster.get_rep()
    targets = 15
    cycles = 4
    docs = 100
    source_range, target_range = 1, (2, targets+1)
    rep.create_dbs(source_range, target_range)
    rep.replicate_1_to_n(source_range, target_range, normal=False)
    for cycle in xrange(1, cycles+1):
        print "\n - cycle", cycle
        time.sleep(QUIET_PERIOD)
        rep.fill(source_range, num=docs)
        rep.wait_till_all_equal(source_range, target_range)
        if cycle == 2:
            print "\n - kill node 1"
            cluster.stop_node(2)
            time.sleep(QUIET_PERIOD+2)

