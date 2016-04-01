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
DEFAULT_PORT = 5984
DEFAULT_REPLICATOR = '_replicator'
REPLICATION_PARAMS = dict(
    #worker_processes = 1,
    #connection_timeout = 200000,
    #http_connections = 1,
    #retries_per_request = 1,
    continuous = True)


def interactive():
    print "Interactive replication toolbox"
    print " rep and couchdb module have been auto-imported"
    print " Assumes cluster runs on http://adm:pass@localhost:5984"
    print " Type rep. and press <TAB> to auto-complete available functions"
    print
    print " Examples:"
    print "  * rep.replicate_1_to_n(10) # replicate 1 source to 10 targets"
    print "  * rep.replciate_n_to_n(2) # replicate 2 sources to 2 targets"
    print "  * rep.getsrv() # get a CouchDB Server instance"
    print "  * rep.getrdb() # get default _replicator database instance"
    print "  * rep.check_untriggered() # check for untriggered docs in _replicator"
    print "  * rep.update_source(2, start=1, end=10) # update source db 2 with 10 docs"
    print
    import IPython
    IPython.start_ipython(argv=["-c","'import rep; import couchdb'","-i"])


def getsrv(srv_or_port=None, timeout=TIMEOUT, full_commit=FULL_COMMIT):
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
    print "Using server ",s.resource.url,"version:",s.version()
    return s


def getdb(db, srv=None, create=True, reset=False):
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
    if db is None:
        db = DEFAULT_REPLICATOR
    return getdb(db, srv=srv, create=create, reset=reset)


def clean_dbs(prefix=PREFIX, srv=None):
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
        yield r.doc


def _batchit(it, batchsize=1):
    if callable(it):
        it = it()
    while True:
        batch = [x for _,x in izip(xrange(batchsize), it)]
        if not batch:
            raise StopIteration
        yield batch


def _bulk_updater(db, docit, batchsize=1000):
    """
    Bulk updater. Takes a db, a document iterator
    and a batchsize. It batches up documents from the
    doc iterator into batches of `batchsize` then calls
    _bulk_docs and yield results one by one as a generator.
    """
    for batch in _batchit(docit, batchsize):
        for (ok, docid, rev) in db.update(batch):
            yield str(ok), str(docid), str(rev)


def clean_docs(prefix, db, startkey=None, endkey=None, srv=None):
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


def create_n_dbs(n, prefix, reset=False, srv=None, filt=None):
    srv = getsrv(srv)
    ddoc_id = '_design/%s' % FILTER_DOC
    if filt is not None:
        filt = get_filter_ddoc(filt)
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


def remote_url(srv, dbname):
    if '://' in dbname:
        return dbname
    url = srv.resource.url
    usr, pwd = srv.resource.credentials
    schema,rest = url.split('://')
    return '://'.join([schema, '%s:%s@%s/%s' % (usr, pwd, rest, dbname)])


def updocs(db, start=1, end=1, prefix=PAYLOAD_PREFIX, **kw):
    start_did = prefix + '%04d' % start
    end_did = prefix + '%04d' % end
    n = clean_docs(prefix=prefix, db=db, startkey=start_did, endkey=end_did)
    if n:
        print "cleaned",n,"docs"
    def dociter():
        for i in range(start, end+1):
            doc = copy.deepcopy(kw)
            doc['_id'] = prefix + '%04d' % i
            yield doc
    print "updating documents..."
    for res in _bulk_updater(db, dociter):
        if res[0]:
            print " >", db.name, res[1], ":", res[2]
        else:
            print " > ERROR:", db.name, res[1], res[2]


def repdoc(src, tgt, srv, filt=None, params=None):
    did = PREFIX + '%04d_%04d' % (src, tgt)
    src_dbname = remote_url(srv, _dbname(src, SRC_PREFIX))
    tgt_dbname = remote_url(srv, _dbname(tgt, TGT_PREFIX))
    if params is None:
        params = {}
    doc = REPLICATION_PARAMS.copy()
    doc.update(_id=did, source=src_dbname, target=tgt_dbname)
    if filt:
        doc['filter'] = '%s/%s' % (FILTER_DOC, FILTER_NAME)
    doc.update(params)
    return doc


def clean_target_dbs(srv=None):
    return clean_dbs(prefix=TGT_PREFIX, srv=srv)


def create_target_dbs(n, reset=False, srv=None):
    create_n_dbs(n=n, prefix=TGT_PREFIX, reset=reset, srv=srv)


def clean_source_dbs(srv=None):
    return clean_dbs(prefix=SRC_PREFIX, srv=srv)


def create_source_dbs(n, reset=False, srv=None, filt=None):
    create_n_dbs(n=n, prefix=SRC_PREFIX, reset=reset, srv=srv, filt=filt)


def clean_replications(rdb=None, srv=None):
    return clean_docs(prefix=PREFIX, db=getrdb(rdb, srv=srv), srv=srv)


def get_filter_ddoc(filt):
    if filt is None:
        return None
    payload = ';' * filt
    assert isinstance(filt, int)
    return {
        "filters": {
            FILTER_NAME : '''function(doc,req) { %s return ; }''' % payload
        }
    }


def update_source(i, srv=None, start=1, end=1, **kw):
    srv = getsrv(srv)
    db = getdb(_dbname(i, SRC_PREFIX), srv=srv, create=False, reset=False)
    return updocs(db=db, start=start, end=end, **kw)


def clean(rdb=None, srv=None):
    srv = getsrv(srv)
    clean_replications(rdb=rdb, srv=srv)
    clean_target_dbs(srv=srv)
    clean_source_dbs(srv=srv)

    
def replicate_n_to_n(n=1, sleep_dt=0, reset=False, srv=None, rdb=None, filt=None, params=None):
    srv = getsrv(srv)
    rdb = getrdb(rdb, srv=srv) # creates replicator db if not there
    create_target_dbs(n, reset=reset, srv=srv)
    print "got",n,"target dbs"
    create_source_dbs(n, reset=reset, srv=srv, filt=filt)
    print "got",n,"source dbs"
    cleaned = clean_replications(rdb=rdb, srv=srv)
    if cleaned:
        print "cleaned",cleaned,"old replication docs"
    def dociter():
        for i in range(1,n+1):
            yield repdoc(src=i, tgt=i, srv=srv, filt=filt, params=params)
    print "updating documents"
    for res in _bulk_updater(rdb, dociter):
        if res[0]:
            print " >", rdb.name, res[1], ":", res[2]
        else:
            print " > ERROR:", rdb.name, res[1], res[2]


def replicate_1_to_n(n=1, sleep_dt=0, reset=False, srv=None, rdb=None, filt=None, params=None):
    srv = getsrv(srv)
    rdb = getrdb(rdb, srv=srv) # creates replicator db if not there
    create_target_dbs(n, reset=reset, srv=srv)
    print "got",n,"target dbs"
    create_source_dbs(1, reset=reset, srv=srv, filt=filt)
    cleaned = clean_replications(rdb=rdb, srv=srv)
    if cleaned:
        print "cleaned",cleaned,"old replication docs"
    def dociter():
        for i in range(1,n+1):
            yield repdoc(src=1, tgt=i, srv=srv, filt=filt, params=params)
    print "updating documents"
    for res in _bulk_updater(rdb, dociter):
        if res[0]:
            print " >", rdb.name, res[1], ":", res[2]
        else:
            print " > ERROR:", rdb.name, res[1], res[2]


def replicate_n_to_1(n=1, sleep_dt=0, reset=False, srv=None, rdb=None, filt=None, params=None):
    srv = getsrv(srv)
    rdb = getrdb(rdb, srv=srv) # creates replicator db if not there
    create_source_dbs(n, reset=reset, srv=srv, filt=filt)
    print "got",n,"source dbs"
    create_target_dbs(1, reset=reset, srv=srv)
    cleaned = clean_replications(rdb=rdb, srv=srv)
    if cleaned:
        print "cleaned",cleaned,"old replication docs"
    def dociter():
        for i in range(1,n+1):
            yield repdoc(src=i, tgt=1, srv=srv, filt=filt, params=params)
    print "updating documents"
    for res in _bulk_updater(rdb, dociter):
        if res[0]:
            print " >", rdb.name, res[1], ":", res[2]
        else:
            print " > ERROR:", rdb.name, res[1], res[2]

def check_untriggered(rdb=None, srv=None):
    srv=getsrv(srv)
    rdb=getrdb(rdb, srv=srv, create=False)
    res = {}
    for doc in _yield_docs(rdb, prefix=PREFIX):
        did = doc.get("_id")
        _replication_state = doc.get('_replication_state')
        if _replication_state != "triggered":
            res[did] = _replication_state
    return res


def tdb(i,srv=None):
    srv = getsrv(srv)
    return getdb(_dbname(i, TGT_PREFIX), srv=srv, create=False)


def tdbdocs(i=1,srv=None):
    res = []
    db = tdb(i,srv=srv)
    for did in db:
        res += [dict(db[did])]
    return res


def rdbdocs(srv=None):
    res = []
    rdb =  getrdb(srv=srv)
    for did in rdb:
        if '_design' in did:
            res += [{'_id':did}]
            continue
        res += [dict(rdb[did])]
    return res
    
        
