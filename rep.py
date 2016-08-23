import time
import copy
import uuid
import couchdb
from itertools import izip
import configargparse


CFG_FILES = ['~/.dyno.cfg']


CFG_DEFAULTS = [
    # --configname, default, env_varname, helpstring
    ('prefix', 'rdyno', 'REP_PREFIX',
     'Prefix used for dbs and docs'),

    ('timeout', 0, 'REP_TIMEOUT',
     'Client socket timeout'),

    ('server_url', 'http://adm:pass@localhost:15984', 'REP_SERVER_URL',
     'Default server URL'),

    ('source_url', None, 'REP_SOURCE_URL',
     'Source URL. If not specified uses server_url'),

    ('target_url', None, 'REP_TARGET_URL',
     'Target URL. If not specified uses server_url'),

    ('replicator_url', None, 'REP_REPLICATOR_URL',
     'Replicator URL. If not specified uses server_url'),

    ('worker_processes', 4, 'REP_WORKER_PROCESSES',
     'Replication parameter'),

    ('connection_timeout', 30000, 'REP_CONNECTION_TIMEOUT',
     'Replicatoin paramater'),

    ('http_connections', 20, 'REP_HTTP_CONNECTIONS',
     'Replication parameter'),

    ('create_target', False, 'REP_CREATE_TARGET',
     'Replication parameter'),
]


FILTER_DOC = 'rdynofilterdoc'
FILTER_NAME = 'rdynofilter'

RETRY_DELAYS = [3,10,20,30,90,180,600]

def replicate_1_to_n_and_compare(*args, **kw):
    r=Rep(cfg = kw.pop('cfg',None))
    r.replicate_1_to_n_and_compare(*args, **kw)


def replicate_n_to_1_and_compare(*args, **kw):
    r=Rep(cfg = kw.pop('cfg',None))
    r.replicate_n_to_1_and_compare(*args, **kw)


def  replicate_n_to_n_and_compare(*args, **kw):
    r=Rep(cfg = kw.pop('cfg',None))
    r.replicate_n_to_n_and_compare(*args, **kw)


def replicate_n_chain_and_compare(*args, **kw):
    r=Rep(cfg = kw.pop('cfg',None))
    r.replicate_n_chain_and_compare(*args, **kw)


def replicate_all_and_compare(*args, **kw):
    r=Rep(cfg = kw.pop('cfg',None))
    r.replicate_all_and_compare(*args, **kw)


def clean(*args, **kw):
    r = Rep(cfg = kw.pop('cfg',None))
    r.clean(*args, **kw)


def getcfg():
    p = configargparse.ArgParser(default_config_files=CFG_FILES)
    for (name, dflt, ev, hs) in CFG_DEFAULTS:
        aname = '--' +name
        if dflt is False:
            p.add_argument(aname, default=dflt, action="store_true", env_var=ev, help=hs)
        else:
            p.add_argument(aname, default=dflt, env_var=ev, help=hs)
    return p.parse_args()


def getsrv(srv=None, timeout=0):
    """
    Get a couchdb.Server() instances. This can usually be passed to all
    subsequent commands.
    """
    if isinstance(srv, couchdb.Server):
        return srv
    elif srv is None:
        cfg = getcfg()
        return couchdb.Server(cfg.server_url)
    elif isinstance(srv, basestring):
        if timeout > 0:
            sess = couchdb.Session(timeout=timeout, retry_delays=RETRY_DELAYS)
            return couchdb.Server(url=srv, session=sess, full_commit=False)
        else:
            sess = couchdb.Session(retry_delays=RETRY_DELAYS)
            return couchdb.Server(url=srv, session=sess, full_commit=False)


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
            print " > deleted db:", dbname
        try:
            r = srv.create(dbname)
            print " > created db:", dbname
            return r
        except couchdb.http.ResourceNotFound:
            print "got resource not found on create",dbname,"retrying..."
            time.sleep(1)
            r = srv.create(dbname)
            print " > created db:", dbname
            return r
    if dbname in srv:
        return srv[dbname]
    else:
        if create:
            r = srv.create(dbname)
            print " > created db:", dbname
            return r
        raise Exception("Db %s does not exist" % dbname)


def getrdb(db=None, srv=None, create=True, reset=False):
    """
    Get replication couchdb.Database() instance. This can be usually
    passed to all the other function.
    """
    if db is None:
        db = "_replicator"
    return getdb(db, srv=srv, create=create, reset=reset)



class Rep(object):

    def __init__(self,cfg=None):
        if cfg is None:
            cfg = getcfg()
        print
        print "configuration:"
        for k,v in sorted(cfg._get_kwargs()):
            print "  ",k,"=",v
        print
        rep_params = {}
        rep_params['worker_processes'] = int(cfg.worker_processes)
        rep_params['connection_timeout'] = int(cfg.connection_timeout)
        rep_params['create_target'] = _2bool(cfg.create_target)
        rep_params['http_connections'] = int(cfg.http_connections)
        self.rep_params = rep_params
        self.prefix = str(cfg.prefix)
        timeout = int(cfg.timeout)
        srv = getsrv(cfg.server_url, timeout=timeout)
        if not cfg.target_url:
            self.tgtsrv = srv
        else:
            self.tgtsrv = getsrv(cfg.target_url, timeout=timeout)
        if not cfg.source_url:
            self.srcsrv = srv
        else:
            self.srcsrv = getsrv(cfg.source_url, timeout=timeout)
        if not cfg.replicator_url:
            self.repsrv = srv
        else:
            self.repsrv = getsrv(cfg.replicator_url, timeout=timeout)
        self.rdb = getrdb(srv=self.repsrv)


    def srcdb(self, i=1):
        return getdb(_dbname(i, self.prefix), srv=self.srcsrv)


    def tgtdb(self, i=1):
        return getdb(_dbname(i, self.prefix), srv=self.tgtsrv)


    def tgtdocs(self,i=1):
        res = []
        db = self.tgtdb(i=i)
        for did in db:
            res += [dict(db[did])]
        return res


    def rdbdocs(self):
        res = []
        db = self.rdb()
        for did in db:
            if '_design' in did:
                res += [{'_id':did}]
                continue
            res += [dict(db[did])]
        return res


    def fill(self, i=1, num=1, rand_ids=False, **kw):
        db = self.srcdb(i=i)
        some_data = uuid.uuid1().hex
        return _updocs(
            db=db,
            num=num,
            prefix=self.prefix,
            rand_ids = rand_ids,
            some_data = some_data,
            **kw)


    def clean(self):
        _clean_docs(prefix=self.prefix, db=self.rdb)
        _clean_dbs(prefix=self.prefix+'_', srv=self.srcsrv)
        _clean_dbs(prefix=self.prefix+'_', srv=self.tgtsrv)


    def create_dbs(self, source_range, target_range, reset=False, filt=None):
        self._create_range_dbs(srv=self.srcsrv, numrange=source_range, reset=reset, filt=None)
        if target_range and not self.rep_params.get('create_target'):
            self._create_range_dbs(srv=self.tgtsrv, numrange=target_range, reset=reset)


    def replicate_n_to_n(self, sr, tr, normal=False, db_per_doc=False, filt=None):
        params = self.rep_params.copy()
        params['continuous'] = not normal
        ipairs = izip(_xrange(sr), _xrange(tr))
        def dociter():
            for s,t in ipairs:
                yield self._repdoc(s, t, filt=filt, params=params)
        return _rdb_updater(self.repsrv, self.rdb, self.prefix, dociter, db_per_doc)


    def replicate_1_to_n(self, sr, tr, normal=False, db_per_doc=False, filt=None):
        params = self.rep_params.copy()
        params['continuous'] = not normal
        xrs, xrt = _xrange(sr), _xrange(tr)
        assert len(xrs) == 1
        assert len(xrt) >= 1
        s = xrs[0]
        def dociter():
            for t in xrt:
                yield self._repdoc(s, t, filt=filt, params=params)
        return _rdb_updater(self.repsrv, self.rdb, self.prefix, dociter, db_per_doc)


    def replicate_n_to_1(self, sr, tr, normal=False, db_per_doc=False, filt=None):
        params = self.rep_params.copy()
        params['continuous'] = not normal
        xrs, xrt = _xrange(sr), _xrange(tr)
        assert len(xrs) >= 1
        assert len(xrt) == 1
        t = xrt[0]
        def dociter():
            for s in xrs:
                yield self._repdoc(s, t, filt=filt, params=params)
        return _rdb_updater(self.repsrv, self.rdb, self.prefix, dociter, db_per_doc)


    def replicate_n_chain(self, sr, tr, normal=False, db_per_doc=False, filt=None):
        params = self.rep_params.copy()
        params['continuous'] = not normal
        xrs = _xrange(sr)
        assert len(xrs) > 1
        assert tr == 0 # target not used
        def dociter():
            prev_s = None
            for i,s in enumerate(xrs):
                if i==0:
                    prev_s = s
                    continue
                else:
                    yield self._repdoc(prev_s, s, filt=filt, params=params)
                    prev_s = s
        return _rdb_updater(self.repsrv, self.rdb, self.prefix, dociter, db_per_doc)


    def replicate_all(self, sr, tr, normal=False, db_per_doc=False, filt=None):
        params = self.rep_params.copy()
        params['continuous'] = not normal
        assert tr == 0 # target not used
        xrs = _xrange(sr)
        assert len(xrs) >= 1
        def dociter():
            for s1 in xrs:
                for s2 in xrs:
                    yield self._repdoc(s1, s2, filt=filt, params=params)
        return _rdb_updater(self.repsrv, self.rdb, self.prefix, dociter, db_per_doc)


    def replicate_1_to_n_and_compare(self, n=1, cycles=1, num=1, normal=False, db_per_doc=False, reset=False):
        sr, tr = 1, (2, n+1)
        repmeth = self.replicate_1_to_n
        def fillcb():
            self.fill(1, num=num)
        self._setup_and_compare(normal, sr, tr, cycles, num, reset, repmeth, fillcb, db_per_doc)


    def replicate_n_to_1_and_compare(self, n=1, cycles=1, num=1, normal=False, db_per_doc=False, reset=False):
        sr, tr = (2, n+1), 1
        repmeth = self.replicate_n_to_1
        def fillcb():
            for src in _xrange(sr):
                self.fill(src, num=num, rand_ids=True)
        self._setup_and_compare(normal, sr, tr, cycles, num, reset, repmeth, fillcb, db_per_doc)


    def replicate_n_to_n_and_compare(self, n=1, cycles=1, num=1, normal=False, db_per_doc=False, reset=False):
        sr, tr  = (1, n), (n+1, 2*n)
        repmeth = self.replicate_n_to_n
        def fillcb():
            for src in _xrange(sr):
                self.fill(src, num=num)
        self._setup_and_compare(normal, sr, tr, cycles, num, reset, repmeth, fillcb, db_per_doc)


    def replicate_n_chain_and_compare(self, n=2, cycles=1, num=1, normal=False, db_per_doc=False, reset=False):
        if n < 2:
            raise ValueError("A chain requires a minimim of 2 nodes")
        sr, tr = (1, n), 0 # target not used here, only sources
        repmeth = self.replicate_n_chain
        def fillcb():
            self.fill(1, num=num)
        self._setup_and_compare(normal, sr, tr, cycles, num, reset, repmeth, fillcb, db_per_doc)


    def replicate_all_and_compare(self, n=1, cycles=1, num=1, normal=False, db_per_doc=False, reset=False):
        sr, tr = (1, n), 0 # target not used here, only sources
        repmeth = self.replicate_all
        def fillcb():
            self.fill(1, num=num)
        self._setup_and_compare(normal, sr, tr, cycles, num, reset, repmeth, fillcb, db_per_doc)


    # Private methods

    def _setup_and_compare(self, normal, sr, tr, cycles, num, reset, rep_method, fill_callback, db_per_doc):
        self._clean_rep_docs(db_per_doc)
        self.create_dbs(sr, tr, reset=reset)
        if normal:
            for cycle in xrange(1, cycles+1):
                if cycles > 1:
                    print
                    print ">>>>>> cycle", cycle, "<<<<<<"
                fill_callback()
                self._clean_rep_docs(db_per_doc)
                rep_method(sr, tr, normal=True, db_per_doc=db_per_doc)
                time.sleep(1)
                self._wait_till_all_equal(sr, tr, log=True)
                _wait_to_complete(rdb=self.rdb, prefix=self.prefix)
        else:
            rep_method(sr, tr, normal=False, db_per_doc=db_per_doc)
            for cycle in xrange(1, cycles+1):
                if cycles > 1:
                    print
                    print ">>>>>> cycle", cycle, "<<<<<<"
                fill_callback()
                time.sleep(1)
                self._wait_till_all_equal(sr, tr, log=True)


    def _wait_till_all_equal(self, sr, tr, log=True):
        t0 = time.time()
        xrs, xrt = _xrange(sr), _xrange(tr)
        if len(xrt) == 0:
            if len(xrs) <= 1:
                return
            prev_s = xrs[0]
            for (i,s) in enumerate(xrs):
                if i==0:
                    prev_s = s
                    continue
                if log:
                    print " > comparing source", prev_s, s
                _wait_to_propagate(self.srcdb(prev_s), self.srcdb(s), self.prefix)
                prev_s = s
        elif len(xrs) == 1 and len(xrt) == 1:
            print " > checking if target and source equal",xrs[0], xrt[0]
            _wait_to_propagate(self.srcdb(xrs[0]), self.tgtdb(xrt[0]), self.prefix)
        elif len(xrs) == 1 and len(xrt) > 1:
            s = xrs[0]
            for t in xrt:
                if log:
                    print " > comparing target ",t
                _wait_to_propagate(self.srcdb(s), self.tgtdb(t), self.prefix)
        elif len(xrs) > 1 and len(xrt) == 1:
            t = xrt[0]
            for s in xrs:
                if log:
                    print " > comparing source",s
                _wait_to_propagate(self.srcdb(s), self.tgtdb(t), self.prefix)
        elif len(xrt) == len(xrs):
            for (s, t) in izip(xrs, xrt):
                if log:
                    print " > comparing source-target pair",s,t
                _wait_to_propagate(self.srcdb(s), self.tgtdb(t), self.prefix)
        else:
            raise ValueError("Cannot compare arbitrary source and target dbs %s %s" % (sr, tr))
        if log:
            dt = time.time() - t0
            print " > changes propagated in at least %.1f sec" % dt


    def _create_range_dbs(self, srv, numrange, reset=None, filt=None):
        if isinstance(numrange, int) or isinstance(numrange, long):
            numrange = (numrange, numrange)
        assert isinstance(numrange, tuple)
        assert len(numrange) == 2
        lo, hi = numrange
        lo = int(lo)
        hi = int(hi)
        if lo > hi:
            lo = hi
        _create_range_dbs(lo, hi, prefix=self.prefix, reset=reset, srv=srv, filt=filt)


    def _repdoc(self, src, tgt, filt, params):
        did = self.prefix + '_%07d_%07d' % (src, tgt)
        src_dbname = _remote_url(self.srcsrv, _dbname(src, prefix=self.prefix))
        tgt_dbname = _remote_url(self.tgtsrv, _dbname(tgt, prefix=self.prefix))
        doc = self.rep_params.copy()
        doc.update(params)
        doc.update(_id=did, source=src_dbname, target=tgt_dbname)
        #print "     ",src_dbname,"->",tgt_dbname
        if filt:
            doc['filter'] = '%s/%s' % (FILTER_DOC, FILTER_NAME)
        return doc


    def _clean_rep_docs(self, db_per_doc):
        prefix = self.prefix + '_repdb_'
        if db_per_doc:
            print "cleaning up replicator dbs prefix:", prefix
            _clean_dbs(prefix=prefix, srv=self.repsrv)
        else:
            print "cleaning existing docs from rep db:", self.rdb.name, "doc prefix:", self.prefix
            _clean_docs(db=self.rdb, srv=self.repsrv, prefix=self.prefix)


# Utility functions


def _interactive():
    """
    IPython interactive prompt launcher. Launches ipython
    (with all its goodies, history of commands, timing modules,
    etc.) and auto-import this module (rep) and also imports
    couchdb python modules.
    """

    print "Interactive replication toolbox"
    print " rep, rep.getsrv, rep.getrdb and couchdb modules are auto-imported"
    print " Assumes cluster runs on http://adm:pass@localhost:5984"
    print " Type rep. and press <TAB> to auto-complete available functions"
    print
    print " Examples:"
    print
    print "  * rep.rep.replicate_1_to_n_and_compare(2, cycles=2)"
    print "    # replicate 1 source to 2 targets (1->2, 1->3). Fill source with data"
    print "    # (add a document) and then wait for all targets to have same data."
    print "    # Do it 2 times (cycles=2)."
    print
    print "  * rep.getsrv() # get a CouchDB Server instance"
    print
    import IPython
    auto_imports = "import rep; from rep import getsrv, getdb, getrdb, Rep; cfg=rep.getcfg(); import couchdb"
    IPython.start_ipython(argv=["-c","'%s'" % auto_imports, "-i"])


class RetryTimeoutExceeded(Exception):
    """Exceed retry timeout"""


def _retry(check=bool, timeout=None, dt=10, log=True):
    """
    Retry a function repeatedly until timeout has passed
    (or forever if timeout > 0). Wait between retries
    is specifed by `dt` parameter. Function is considered
    to have succeeded if it didn't throw an exception and
    then based `check` param:

      * if check is callable then it calls check(result) and
        if that returns True it is considered to have succeeded.

      * if check is not a calleble then checks for equality
    If timeout is exceeded then RetryTimeoutExceeded exception
    is raised.

    Example:
     @_retry(2, timeout=10, 1.5)
     def fun(...):
         ...
     Function is retried for 10 seconds with 1.5 seconds interval
     in between. If it returns true then it succeeds. If it doesn't
     return true then RetryTimeoutExceeded exception will be
     thrown.
    """
    if dt<=0:
        dt=0.001
    def deco(f):
        def retry(*args, **kw):
            t0 = time.time()
            tf = t0 + timeout if timeout > 0 else None
            t = 1
            while True:
                if tf is not None and time.time() > tf:
                    raise RetryTimeoutExceeded("Timeout : %s" % timeout)
                try:
                    r = f(*args, **kw)
                except Exception,e:
                    fn = _fname(f)
                    print " > function", fn, "threw exception", e, "retrying"
                    t += 1
                    time.sleep(dt * 2**t)
                    continue
                t = 1
                if (callable(check) and check(r)) or check==r:
                    if log:
                        fn = _fname(f)
                        tstr = '%.1f +/- %.1f ' % (time.time()-t0, dt)
                        print " > function", fn, "succeded after", tstr, "sec."
                    return r
                elif check == r:
                    return r
                if log:
                    print " > retrying function", _fname(f)
                time.sleep(dt)
        return retry
    return deco


def _clean_dbs(prefix, srv):
    srv = getsrv(srv)
    cnt = 0
    for dbname in srv:
        if dbname.startswith(prefix):
            del srv[dbname]
            print "   > deleted db:",dbname
            cnt += 1
    return cnt


def _xrange(r):
    if isinstance(r, int) or isinstance(r, long):
        if r<=0:
            return ()
        r = (r,r)
    assert isinstance(r, tuple)
    assert len(r) == 2
    assert r[0] <= r[1]
    return xrange(r[0], r[1]+1)



def _updocs(db, num, prefix, rand_ids, **kw):
    """
    Update a set of docs in a database using an incremental
    scheme with a prefix.
    """
    start, end = 1, num
    _clean_docs(prefix=prefix, db=db, startkey=prefix+'_', endkey=prefix+'_zzz')
    def dociter():
        for i in range(start, end+1):
            doc = copy.deepcopy(kw)
            _id = prefix + '_%07d' % i
            if rand_ids:
                _id += '_' + uuid.uuid4().hex
            doc['_id'] = _id
            yield doc
    for res in _bulk_updater(db, dociter):
        if res[0]:
            print " > updated doc", db.name,  res[1], res[2]
            pass
        else:
            print " > ERROR:", db.name, res[1], res[2]


def _yield_revs(db, prefix=None, all_docs_params=None, batchsize=1000):
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


def _rdb_updater(repsrv, rdb, prefix, dociter, db_per_doc):
    if db_per_doc:
        for n, doc in enumerate(dociter()):
            _rdb_and_doc(repsrv, prefix, n, doc)
        return
    ok, fail = 0, 0
    for res in _bulk_updater(rdb, dociter):
        if res[0]:
            ok += 1
        else:
            fail += 1
            print "  ! ERROR:", rdb.name, res[1], res[2]


def _rdb_and_doc(rdbsrv, prefix, n, doc):
    dbname = prefix + '_repdb_%07d' % n + '/_replicator'
    db = getdb(dbname, srv=rdbsrv)
    db[doc['_id']] = doc
    return doc

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
    for res in _bulk_updater(db, dociter, batchsize=5000):
        print " > deleted doc:", res[1], res[0]
        cnt += 1
    return cnt


def _dbname(num, prefix):
    return prefix + '_%07d' % num


def _fname(f):
    try:
       return f.func_name
    except:
       return str(f)


def _create_range_dbs(lo, hi, prefix, reset=False, srv=None, filt=None):
    srv = getsrv(srv)
    ddoc_id = '_design/%s' % FILTER_DOC
    if filt is not None:
        filt = _filter_ddoc(filt)
    existing_dbs = set(srv)
    want_dbs = set((_dbname(i, prefix) for i in xrange(lo, hi+1)))
    if reset:
        found_dbs = list(want_dbs & existing_dbs)
        found_dbs.sort()
        for dbname in found_dbs:
            del srv[dbname]
            print "  > deleted db:", dbname
        missing_dbs = want_dbs
    else:
        missing_dbs = want_dbs - existing_dbs
    if len(missing_dbs) == 0:
        return
    missing_list = list(missing_dbs)
    missing_list.sort()
    print "Creating",len(missing_list),"databases"
    #t0 = time.time()
    for dbname in missing_list:
        db = srv.create(dbname)
        print "  > created",dbname
        _maybe_add_filter(db=db, ddoc_id=ddoc_id, filtdoc=filt)
    #dt = time.time() - t0
    #print "Finished creating", len(missing_list), "dbs in %.1f sec" % dt


def _maybe_add_filter(db, ddoc_id, filtdoc):
    if not filtdoc:
        return
    filt2 = copy.deepcopy(filtdoc)
    if ddoc_id in db:
        oldd = db[ddoc_id]
        rev = oldd['_rev']
        filt2['_rev'] = rev
    db[ddoc_id] = filt2


def _remote_url(srv, dbname):
    if '://' in dbname:
        return dbname
    url = srv.resource.url
    usr, pwd = srv.resource.credentials
    schema,rest = url.split('://')
    return '://'.join([schema, '%s:%s@%s/%s' % (usr, pwd, rest, dbname)])


def _filter_ddoc(filt):
    if filt is None:
        return None
    if isinstance(filt, (int,long)):
        payload = ';' * filt
        filter_str = '''function(doc,req) { %s return ; }''' % payload
    else:
        assert isinstance(filt, basestring)
        filter_str = filt
    return {
        "filters": {
            FILTER_NAME : filter_str
        }
    }


def _get_incomplete(rdb, prefix):
    res = {}
    for doc in _yield_docs(rdb, prefix=prefix):
        did = doc.get("_id")
        _replication_state = doc.get('_replication_state','')
        if _replication_state == "completed":
            continue
        res[str(did)] = str(_replication_state)
    return res


@_retry(lambda x : x == {}, 24*3600, 10, False)
def _wait_to_complete(rdb, prefix):
    return _get_incomplete(rdb=rdb, prefix=prefix)


def _contains(db1, db2, prefix):
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
    # first compare ids only, if those show differences, no reason to bother
    # with getting all the docs
    s1 = set((_id[0] for _id in _yield_revs(db1, prefix=prefix, batchsize=1000)))
    s2 = set((_id[0] for _id in _yield_revs(db2, prefix=prefix, batchsize=1000)))
    sdiff12 = s1 - s2
    if sdiff12:
        return False
    dict2 = {}
    for d2doc in _yield_docs(db2, prefix=prefix):
        d2doc.pop('_rev')
        _id = d2doc['_id']
        dict2[_id] = d2doc
    for d1doc in _yield_docs(db1, prefix=prefix):
        d1doc.pop('_rev')
        _id = d1doc['_id']
        if _id not in dict2:
            return False
        if dict2[_id] != d1doc:
            return False
    return True


@_retry(True, 24*3600, 10, False)
def _wait_to_propagate(db1, db2, prefix):
    return _contains(db1=db1, db2=db2, prefix=prefix)


def _2bool(v):
    if isinstance(v, bool):
        return v
    elif isinstance(v, basestring):
        if v.strip().lower() == 'true':
            return True
        return False
    else:
        raise ValueError("Invalid boolean value: %s" % v)


