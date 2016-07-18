#!/usr/bin/python

# Replication toolbox library to be used from ipython
# (or basic Python prompt).

import time, copy, couchdb  # pip : CouchDB==1.0
from itertools import izip

PREFIX='rdyno_'
SRC_PREFIX=PREFIX+'src_'
TGT_PREFIX=PREFIX+'tgt_'
PAYLOAD_PREFIX=PREFIX+'data_'
FILTER_DOC='rdynofilterdoc'
FILTER_NAME='rdynofilter'
TIMEOUT=None #300 # seconds
FULL_COMMIT=False
DEFAULT_HOST = 'http://adm:pass@localhost'
DEFAULT_PORT = 15984
DEFAULT_REPLICATOR = '_replicator'
REPLICATION_PARAMS = dict(
    worker_processes = 1,
    connection_timeout = 120000,
    http_connections = 1,
    retries_per_request = 2,
    continuous = True)


def interactive():
    """
    IPython interactive prompt launcher. Launches ipython
    (with all its goodies, history of commands, timing modules,
    etc.) and auto-import this module (rep) and also imports
    couchdb python modules.
    """
    print "Interactive replication toolbox"
    print " rep and couchdb module have been auto-imported"
    print " Assumes cluster runs on http://adm:pass@localhost:5984"
    print " Type rep. and press <TAB> to auto-complete available functions"
    print
    print " Examples:"
    print "  * rep.replicate_1_to_n(10) # replicate 1 source to 10 targets"
    print "  * rep.replicate_n_to_n(20) # replicate 20 sources to 20 targets, 1->1, 2->2, etc"
    print "  * rep.getsrv() # get a CouchDB Server instance"
    print "  * rep.replicate_1_to_n_then_check_replication(10, cycles=3, num=10)"
    print "    # replicate 1 source to 10 target"
    print "    # then update source with 10 docs and wait till they appear on target"
    print "    # do that for 3 cycles in a row."
    print
    import IPython
    IPython.start_ipython(argv=["-c","'import rep; import couchdb'","-i"])


def getsrv(srv_or_port=None, timeout=TIMEOUT, full_commit=FULL_COMMIT):
    """
    Get a couchdb.Server() instances. This can usually be passed to all
    subsequent commands.
    """
    if isinstance(srv_or_port, couchdb.Server):
        return srv_or_port
    elif isinstance(srv_or_port, int):
        url = DEFAULT_HOST+':%s' % srv_or_port
    elif srv_or_port is None:
        url = DEFAULT_HOST+':%s' % DEFAULT_PORT
    elif isinstance(srv_or_port, basestring):
        url = srv_or_port
    if timeout is not None:
        sess = couchdb.Session(timeout=TIMEOUT)
        s = couchdb.Server(url, session=sess, full_commit=full_commit)
    else:
        s = couchdb.Server(url, full_commit=full_commit)
    #print "Using server ",s.resource.url,"version:",s.version()
    return s


def getdb(db, srv=None, create=True, reset=False):
    """
    Get a couchdb.Database() instance. This can be used to manipulate
    documents in a database.
    """
    if isinstance(db, couchdb.Database):
        return db
    dbname = db
    srv = getsrv(srv)
    if reset:
        if dbname in srv:
            del srv[dbname]
        print "creating",dbname
        try:
            return srv.create(dbname)
        except couchdb.http.ResourceNotFound:
            print "got resource not found on create",dbname,"retrying..."
            time.sleep(1)
            return srv.create(dbname)
    if dbname in srv:
        return srv[dbname]
    else:
        if create:
            print "creating",dbname
            return srv.create(dbname)
        raise Exception("Db %s does not exist" % dbname)


def getrdb(db=None, srv=None, create=True, reset=False):
    """
    Get replication couchdb.Database() instance. This can be usually
    passed to all the other function.
    """
    if db is None:
        db = DEFAULT_REPLICATOR
    return getdb(db, srv=srv, create=create, reset=reset)

class RetryTimeoutExceeded(Exception):
    """Exceed retry timeout"""

def retry_till(check=bool, timeout=None, dt=1, log=True):
    """
    Retry a function repeatedly until timeout has passed
    (or forever if timeout is None). Wait between retries
    is specifed by `dt` parameter. Function is considered
    to have succeeded if it didn't throw an exception and
    then based `check` param:

      * if check is callable then it calls check(result) and
        if that returns True it is considered to have succeeded.

      * if check is not a calleble then checks for equality
    If timeout is exceeded then RetryTimeoutExceeded exception
    is raised.

    Example:
     @retry_till(2, timeout=10, 1.5)
     def fun(...):
         ...
     Function is retried for 10 seconds with 1.5 seconds interval
     in between. If it returns 2 then it succeeds. If it doesn't
     return true then RetryTimeoutExceeded exception will be
     thrown.
    """
    def deco(f):
        def retry(*args, **kw):
            t0 = time.time()
            tf = t0 + timeout if timeout is not None else None
            while True:
                if tf is not None and time.time() > tf:
                    raise RetryTimeoutExceeded("Timeout : %s" % timeout)
                r = f(*args, **kw)
                if (callable(check) and check(r)) or check==r:
                    if log:
                        fn = _fname(f)
                        tstr = '%.3f +/- %.1f ' % (time.time()-t0, dt)
                        print " > function", fn, "succeded after", tstr, "sec."
                    return r
                elif check == r:
                    return r
                if log:
                    print " > retrying function", _fname(f)
                time.sleep(dt)
        return retry
    return deco


def updocs(db, num=1, prefix=PAYLOAD_PREFIX, **kw):
    """
    Update a set of docs in a database using an incremental
    scheme with a prefix.
    """
    start, end = 1, num
    start_did = prefix + '%04d' % start
    end_did = prefix + '%04d' % end
    _clean_docs(prefix=prefix, db=db, startkey=start_did, endkey=end_did)
    def dociter():
        for i in range(start, end+1):
            doc = copy.deepcopy(kw)
            doc['_id'] = prefix + '%04d' % i
            yield doc
    #print "updating documents..."
    for res in _bulk_updater(db, dociter):
        if res[0]:
            pass # print " >", db.name, res[1], ":", res[2]
        else:
            print " > ERROR:", db.name, res[1], res[2]


def update_source(i=1, num=1, srv=None, **kw):
    """
    Update a particular source identified by a id (default=1) with
    a range of documents starting at start (default=1) and inclusive end
    (default=1). Additional keyword parameters can be provides which
    will end up being written to the source docs
    """
    srv = getsrv(srv)
    db = getdb(_dbname(i, SRC_PREFIX), srv=srv, create=False, reset=False)
    return updocs(db=db, num=num, **kw)


def clean(rdb=None, srv=None):
    """
    Clean replication docs, then clean targets and sources
    """
    srv = getsrv(srv)
    _clean_docs(prefix=PREFIX, db=getrdb(rdb, srv=srv), srv=srv)
    _clean_dbs(prefix=TGT_PREFIX, srv=srv)
    _clean_dbs(prefix=SRC_PREFIX, srv=srv)


def replicate_n_to_n(n=1, reset=False, srv=None, rdb=None, filt=None, params=None):
    """
    Create n to n replications. Source 1 replicates to target 1, source 2 to target 2, etc.
    filt parameter can specify a size of a filter, to emulate fetching a large filter.
    """
    srv = getsrv(srv)
    rdb = getrdb(rdb, srv=srv)
    _create_n_dbs(n=n, prefix=SRC_PREFIX, reset=reset, srv=srv, filt=filt)
    _create_n_dbs(n=n, prefix=TGT_PREFIX, reset=reset, srv=srv)
    _clean_docs(prefix=PREFIX, db=rdb, srv=srv)
    def dociter():
        for i in xrange(1,n+1):
            yield _repdoc(src=i, tgt=i, srv=srv, filt=filt, params=params)
    return _rdb_bulk_updater(rdb, dociter)


def replicate_1_to_n(n=1, reset=False, srv=None, rdb=None, filt=None, params=None):
    """
    Create 1 to n replications. One source replicates to n different targets.
    filt parameter can specify a size of a filter, to emulate fetching a large filter.
    """
    srv = getsrv(srv)
    rdb = getrdb(rdb, srv=srv)
    _create_n_dbs(n=1, prefix=SRC_PREFIX, reset=reset, srv=srv, filt=filt)
    _create_n_dbs(n=n, prefix=TGT_PREFIX, reset=reset, srv=srv)
    _clean_docs(prefix=PREFIX, db=rdb, srv=srv)
    def dociter():
        for i in xrange(1,n+1):
            yield _repdoc(src=1, tgt=i, srv=srv, filt=filt, params=params)
    return _rdb_bulk_updater(rdb, dociter)


def replicate_n_to_1(n=1, reset=False, srv=None, rdb=None, filt=None, params=None):
    """
    Replicate n to 1. n sources replicates to 1 single target.
    filt parameter can specify a size of a filter, to emulate fetching a large filter.
    """
    srv = getsrv(srv)
    rdb = getrdb(rdb, srv=srv) # creates replicator db if not there
    _create_n_dbs(n=n, prefix=SRC_PREFIX, reset=reset, srv=srv, filt=filt)
    _create_n_dbs(n=1, prefix=TGT_PREFIX, reset=reset, srv=srv)
    _clean_docs(prefix=PREFIX, db=rdb, srv=srv)
    def dociter():
        for i in xrange(1,n+1):
            yield _repdoc(src=i, tgt=1, srv=srv, filt=filt, params=params)
    return _rdb_bulk_updater(rdb, dociter)


def tdb(i=1, srv=None):
    """
    Get target db object identified by an id (starting at 1)
    """
    srv = getsrv(srv)
    return getdb(_dbname(i, TGT_PREFIX), srv=srv, create=False)


def tdbdocs(i=1,srv=None):
    """
    Return list of all docs from a particular target db identifeid
    by an id (starting at 1)
    """
    res = []
    db = tdb(i,srv=srv)
    for did in db:
        res += [dict(db[did])]
    return res


def rdbdocs(srv=None, rdb=None):
    """
    Return list of replicator docs. Skips over _design/
    docs.
    """
    res = []
    rdb =  getrdb(srv=srv, db=rdb)
    for did in rdb:
        if '_design' in did:
            res += [{'_id':did}]
            continue
        res += [dict(rdb[did])]
    return res

def compare_dbs(db1, db2, srv=None, prefix=None):
    """
    Compare 2 databases, optionally only compare documents with
    a certain prefix.
    Return:
       A lower bound on (num_docs_in_1_but_not_2,  num_docs_in_2_but_not_1).
    The lower bounds is because of an optimization -- in case prefix is
    not specified, if databases simply have a different number of docs,
    then return the difference in the number of docs. Then if number match,
    specific document _ids are compared using a set difference algorithm. Only
    if those match, it will fetch all docs and compare that way.
    """
    srv = getsrv(srv)
    if isinstance(db1, basestring):
        db1 = srv[db1]
    if isinstance(db2, basestring):
        db2 = srv[db2]
    if prefix is None:
        # if prefix is not specified, look at number of docs only as first try
        cnt1, cnt2 = db1.info()['doc_count'], db2.info()['doc_count']
        if cnt1 != cnt2:
            return cnt1-cnt2, cnt2-cnt1
    # next, compare ids only, if those show differences, no reason to bother
    # with getting all the docs
    s1 = set((_id for _id in _yield_revs(db1, prefix=prefix, batchsize=1000)))
    s2 = set((_id for _id in _yield_revs(db2, prefix=prefix, batchsize=1000)))
    if len(s1) != len(s2):
        return len(s1)-len(s2), len(s2)-len(s1)
    sdiff12, sdiff21 = s1-s2, s2-s1
    if sdiff12 or sdiff21:
        return len(sdiff12), len(sdiff21)
    # do the slow thing, this builds all docs in memory so don't use on large dbs
    dict1, dict2 = {}, {}
    sdocdiff12, sdocdiff21 = set(), set()
    for d1doc in _yield_docs(db1, prefix=None):
        d1doc.pop('_rev')
        dict1[d1doc['_id']] = d1doc
    for d2doc in _yield_docs(db2, prefix=None):
        d2doc.pop('_rev')
        _id = d2doc['_id']
        dict2[_id] = d2doc
        if _id not in dict1 or d2doc!=dict1[_id]:
            sdocdiff21.add(_id)
    for _id, d1doc in dict1.iteritems():
        if _id not in dict2 or d1doc != dict2[_id]:
            sdocdiff12.add(_id)
    return len(sdocdiff12), len(sdocdiff21)


@retry_till((0,0), 1200, 1)
def wait_till_dbs_equal(db1, db2, srv=None, prefix=None):
    return compare_dbs(db1, db2, srv=None, prefix=None)


def wait_till_source_and_target_equal(src_id, target_id, srv=None, log=True):
    srv = getsrv(srv)
    src = getdb(_dbname(src_id,    SRC_PREFIX), srv=srv, create=False)
    tgt = getdb(_dbname(target_id, TGT_PREFIX), srv=srv, create=False)
    t0 = time.time()
    wait_till_dbs_equal(src, tgt, srv=srv)
    dt = time.time()-t0
    if log:
        print " > waiting till source_and_target equal %s, %s waited %.3f seconds" % (
            src_id, target_id, dt)


def wait_till_n_targets_equal_source(targets, src_id, srv=None, log=True):
    t0 = time.time()
    for i in xrange(1, targets+1):
        print " > waiting for target ",i
        wait_till_source_and_target_equal(src_id=src_id, target_id=i, srv=srv, log=False)
    dt = time.time()-t0
    if log:
        print " > waiting to propagate changes from ",src_id,"to",targets," : %.3f sec."%dt


def update_source_and_wait_to_propagate_to_targets(targets, src_id=1, num=1, srv=None, log=True, **kw):
    update_source(i=src_id, num=num, srv=srv, **kw)
    wait_till_n_targets_equal_source(targets=targets, src_id=src_id, srv=srv, log=log)


def replicate_1_to_n_then_check_replication(n=1, cycles=1, num=1, reset=False,  srv=None, rdb=None, filt=None):
    replicate_1_to_n(n=n, reset=reset, srv=srv, rdb=rdb, filt=filt)
    for cycle in xrange(cycles):
        print ">>> update cycle",cycle," <<<"
        update_source_and_wait_to_propagate_to_targets(targets=n, num=num, srv=srv, log=True, cycle=cycle)
        print


def replicate_1_to_n_normal(n, rdb, srv, filt, params=None):
    if params is None:
        params = {'continuous': False}
    else:
        params['continuous'] = False
    def dociter():
        for i in xrange(1,n+1):
            yield _repdoc(src=1, tgt=i, srv=srv, filt=filt, params=params)
    _clean_docs(prefix=PREFIX, db=rdb, srv=srv)
    _rdb_bulk_updater(rdb, dociter)


def update_source_then_replicate_1_to_n_normal_replications_and_wait_to_complete(
        n=1, cycles=1, num=1, reset=False, srv=None, rdb=None, filt=None):
    srv = getsrv(srv)
    rdb = getrdb(rdb, srv=srv)
    _create_n_dbs(n=1, prefix=SRC_PREFIX, reset=reset, srv=srv, filt=filt)
    _create_n_dbs(n=n, prefix=TGT_PREFIX, reset=reset, srv=srv)
    for cycle in xrange(1, n+1):
        if (n>1):
            print ">>> update cycle",cycle,"<<<"
        update_source(i=1, num=num, srv=srv)
        replicate_1_to_n_normal(n=n, srv=srv, rdb=rdb, filt=filt)
        wait_to_complete(rdb=rdb)
        wait_till_n_targets_equal_source(targets=n, src_id=1, srv=srv, log=True)


### Private Utility Functions ###

def _clean_dbs(prefix=PREFIX, srv=None):
    srv = getsrv(srv)
    cnt = 0
    for dbname in srv:
        if dbname.startswith(prefix):
            del srv[dbname]
            print "deleted db",dbname
            cnt += 1
    return cnt


def _yield_revs(db, prefix=None, all_docs_params=None, batchsize=2000):
    """
    Read doc revisions from db (with possible prefix filtering)
    and yield tuples of (_id, rev). Do it in an efficient
    manner using batching.
    """
    if all_docs_params is None:
        all_docs_params = {}
    all_docs_params['batch'] = batchsize
    for r in db.iterview('_all_docs', **all_docs_params):
        _id = str(r.id)
        if prefix and not _id.startswith(prefix):
            continue
        yield (str(r.id), str(r.value['rev']))


def _yield_docs(db, prefix=None, batchsize=1000):
    """
    Read docs from db (with possible prefix filtering)
    and yield docs
    """
    for r in db.iterview('_all_docs', batch=batchsize, include_docs=True):
        _id = str(r.id)
        if prefix and not _id.startswith(prefix):
            continue
        yield dict(r.doc)


def _batchit(it, batchsize=500):
    """
    This is a batcher. Given an interator and a batchsize,
    generate lists of up to batchsize items. When done
    raises StopItration exception.
    """
    if callable(it):
        it = it()
    while True:
        batch = [x for _,x in izip(xrange(batchsize), it)]
        if not batch:
            raise StopIteration
        yield batch


def _bulk_updater(db, docit, batchsize=500):
    """
    Bulk updater. Takes a db, a document iterator
    and a batchsize. It batches up documents from the
    doc iterator into batches of `batchsize` then calls
    _bulk_docs and yield results one by one as a generator.
    """
    for batch in _batchit(docit, batchsize):
        for (ok, docid, rev) in db.update(batch):
            yield str(ok), str(docid), str(rev)

def _rdb_bulk_updater(rdb, dociter):
    print "updating documents"
    for res in _bulk_updater(rdb, dociter):
        if res[0]:
            print " >", rdb.name, res[1], ":", res[2]
        else:
            print " > ERROR:", rdb.name, res[1], res[2]

def _clean_docs(prefix, db, startkey=None, endkey=None, srv=None):
    db = getdb(db, srv=srv, create=False, reset=False)
    if startkey is not None and endkey is not None:
        all_docs_params = dict(startkey=startkey,
                               endkey=endkey,
                               inclusive_end=True)
    else:
        all_docs_params = None
    doc_revs = _yield_revs(db, prefix=prefix, all_docs_params=all_docs_params)
    def dociter():
        for _id,_rev in doc_revs:
            yield dict(_id=_id, _rev=_rev, _deleted=True)
    cnt = 0
    for res in _bulk_updater(db, dociter, batchsize=2000):
        cnt += 1
    return cnt


def _dbname(num, prefix):
    return prefix + '%04d' % num

def _fname(f):
    try:
       return f.func_name
    except:
       return str(f)

def _create_n_dbs(n, prefix, reset=False, srv=None, filt=None):
    srv = getsrv(srv)
    ddoc_id = '_design/%s' % FILTER_DOC
    if filt is not None:
        filt = _filter_ddoc(filt)
    for i in range(1,n+1):
        dbname = _dbname(i, prefix)
        db = getdb(dbname, srv=srv, create=True, reset=reset)
        if filt:
            if ddoc_id in db:
                oldd = db['ddoc_id']
                rev = oldd['_rev']
                filt2 = copy.deepcopy(filt)
                filt2['_rev'] = rev
                db[ddoc_id] = filt2
            else:
                db[ddoc_id] = copy.deepcopy(filt)
    print " > created ",n,"dbs with prefix",prefix


def _remote_url(srv, dbname):
    if '://' in dbname:
        return dbname
    url = srv.resource.url
    usr, pwd = srv.resource.credentials
    schema,rest = url.split('://')
    return '://'.join([schema, '%s:%s@%s/%s' % (usr, pwd, rest, dbname)])


def _repdoc(src, tgt, srv, filt=None, params=None):
    did = PREFIX + '%04d_%04d' % (src, tgt)
    src_dbname = _remote_url(srv, _dbname(src, SRC_PREFIX))
    tgt_dbname = _remote_url(srv, _dbname(tgt, TGT_PREFIX))
    if params is None:
        params = {}
    doc = REPLICATION_PARAMS.copy()
    doc.update(_id=did, source=src_dbname, target=tgt_dbname)
    if filt:
        doc['filter'] = '%s/%s' % (FILTER_DOC, FILTER_NAME)
    doc.update(params)
    return doc

def _filter_ddoc(filt):
    if filt is None:
        return None
    payload = ';' * filt
    assert isinstance(filt, int)
    return {
        "filters": {
            FILTER_NAME : '''function(doc,req) { %s return ; }''' % payload
        }
    }


def _get_incomplete(rdb):
    res = {}
    for doc in _yield_docs(rdb, prefix=PREFIX):
        did = doc.get("_id")
        _replication_state = doc.get('_replication_state','')
        if _replication_state == "completed":
            continue
        res[str(did)] = str(_replication_state)
    return res



@retry_till(lambda x : x == {}, 600, 10)
def wait_to_complete(rdb=None):
    return _get_incomplete(rdb=rdb)

