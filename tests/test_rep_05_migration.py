import time
import conftest


# rep is a pytest fixture from conftest.py


def test_migration_error(rep):
    conftest.skip_if_no_replication_scheduler(rep)
    rdb = rep.rdb
    rep_params = {
        '_replication_state': 'error',
        '_replication_state_reason': 'snek',
        'other_random': 'data'
    }
    rep.replicate_1_to_n_and_compare(1, num=1, normal=False, rep_params=rep_params)
    time.sleep(10)
    rdoc = rdb['rdyno-0000001-0000002']
    assert '_replication_state' not in rdoc
    assert '_replication_state_reason' not in rdoc
    assert rdoc['other_random'] == 'data'


def test_migration_triggered(rep):
    conftest.skip_if_no_replication_scheduler(rep)
    rdb = rep.rdb
    rep_params = {
        '_replication_state': 'triggered',
        '_replication_state_time': 'sometime',
        'other_random': 'data'
    }
    rep.replicate_1_to_n_and_compare(1, num=1, normal=False, rep_params=rep_params)
    time.sleep(10)
    rdoc = rdb['rdyno-0000001-0000002']
    assert '_replication_state' not in rdoc
    assert '_replication_state_reason' not in rdoc
    assert rdoc['other_random'] == 'data'


def test_migration_downgrade_failed(rep):
    """
    Test downgrade of a document which was marked as failed
    """
    conftest.skip_if_replication_scheduler(rep)
    rdb = rep.rdb
    rep_params = {
        '_replication_state': 'failed',
        '_replication_state_reason': 'newscheduler',
    }
    rep.replicate_1_to_n_and_compare(1, num=1, normal=False, rep_params=rep_params)
    time.sleep(10)
    rdoc = rdb['rdyno-0000001-0000002']
    assert '_replication_state' in rdoc
    assert rdoc['_replication_state'] == 'triggered'
