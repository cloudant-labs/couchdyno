import time
import conftest


# running_local_cluster is a pytest fixture from conftest.py

@conftest.skip_if_not_local
def test_migration_on_node_failure(running_local_cluster):
    """
    Using a cluster with a short replicator cluster quiet period
    (to avoid waiting for too long):
       - create 1-to-n replications
       - fill source db 100 documents
       - after a few cycles kill node 2
       - verify all changes are still replicating
    """
    cluster = running_local_cluster
    rep = cluster.get_rep()
    targets = 15
    cycles = 4
    docs = 100
    source_range, target_range = 1, (2, targets+1)
    rep.create_dbs(source_range, target_range)
    rep.replicate_1_to_n(source_range, target_range, normal=False)
    for cycle in xrange(1, cycles+1):
        print "\n - cycle", cycle
        time.sleep(conftest.QUIET_PERIOD+2)
        rep.fill(source_range, num=docs)
        rep.wait_till_all_equal(source_range, target_range)
        if cycle == 2:
            print "\n - kill node 1"
            cluster.stop_node(2)
