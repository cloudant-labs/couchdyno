import time
import copy
import couchdb
from itertools import izip
import configargparse


CFG_FILES = ['~/.dyno_rep.cfg']


CFG_DEFAULTS = [
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

    ('fast_comparison', False, 'REP_FAST_COMPARISON',
     'If set, compare only by document id, not document contents'),

    ('skip_db_check', False, 'REP_SKIP_DB_CHECK',
     'If set set do not check if source / target dbs need to be created'),

    ('worker_processes', 1, 'REP_WORKER_PROCESSES',
     'Replication parameter'),

    ('connection_timeout', 140000, 'REP_CONNECTION_TIMEOUT',
     'Replicatoin paramater'),

    ('http_connections', 1, 'REP_HTTP_CONNECTIONS',
     'Replication parameter'),

    ('create_target', False, 'REP_CREATE_TARGET',
     'Replication parameter'),
]


FILTER_DOC = 'rdynofilterdoc'
FILTER_NAME = 'rdynofilter'

class Rep(object):

    def __init__(self,cfg=None):
        if cfg is None:
            cfg = _getcfg()
        rep_params = {}
        rep_params['worker_processes'] = int(cfg.worker_processes)
        rep_params['connection_timeout'] = int(cfg.connection_timeout)
        rep_params['create_target'] = _2bool(cfg.create_target)
        rep_params['http_connections'] = int(cfg.http_connections)
        self.rep_params = rep_params
        self.prefix = str(cfg.prefix)
        self.skip_db_check = _2bool(cfg.skip_db_check)
        self.fast_comparison = _2bool(cfg.fast_comparison)
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
        return getdb(_dbname(i, self.prefix+'_src'))


    def tgtdb(self, i=1):
        return getdb(_dbname(i, self.prefix+'_tgt'))


    def tdbdocs(self,i=1):
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


    def fill(self, i=1, num=1, **kw):
        db = self.srcdb(i=i)
        return _updocs(db=db, num=num, prefix=self.prefix, **kw)


    def clean(self):
        _clean_docs(prefix=self.prefix, db=self.rdb)
        _clean_dbs(prefix=_src_dbname(None, self.prefix), srv=self.srcsrv)
        _clean_dbs(prefix=_tgt_dbname(None, self.prefix), srv=self.tgtsrv)


    def create_dbs(self, sources, targets, reset=False, filt=None):
        self._create_n_src_dbs(sources, reset=reset, filt=None)
        if not self.rep_params.get('create_target'):
            self._create_n_tgt_dbs(targets, reset=reset)


    def replicate_n_to_n(self, n=1, normal=False, filt=None):
        params = self.rep_params.copy()
        if normal:
            params['continuous'] = False
        else:
            params['continuous'] = True
        def dociter():
            for i in xrange(1, n+1):
                yield self._repdoc(src=i, tgt=i, filt=filt, params=params)
        return _rdb_bulk_updater(self.rdb, dociter)


    def replicate_1_to_n(self, n=1, normal=False, filt=None):
        params = self.rep_params.copy()
        if normal:
            params['continuous'] = False
        else:
            params['continuous'] = True
        def dociter():
            for i in xrange(1, n+1):
                yield self._repdoc(src=1, tgt=i, filt=filt, params=params)
        return _rdb_bulk_updater(self.rdb, dociter)


    def replicate_n_to_1(self, n=1, normal=False, filt=None):
        params = self.rep_params.copy()
        if normal:
            params['continuous'] = False
        else:
            params['continuous'] = True
        def dociter():
            for i in xrange(1, n+1):
                yield self._repdoc(src=i, tgt=1, filt=filt, params=params)
        return _rdb_bulk_updater(self.rdb, dociter)


    def replicate_1_to_n_continuous_and_wait_till_equal(self, n=1, cycles=1, num=1, reset=False):
        self.create_dbs(1, n, reset=reset)
        _clean_docs(db=self.rdb, srv=self.repsrv, prefix=self.prefix)
        self.replicate_1_to_n(n=n, normal=False)
        for cycle in xrange(cycles):
            if cycles > 1:
                print ">>> update cycle", cycle
            self.fill(i=1, num=num)
            self._wait_till_all_equal(sources=1, targets=n, log=True)


    def replicate_1_to_n_normal_and_wait_till_equal(self, n=1, cycles=1, num=1, reset=False):
        self.create_dbs(1, n, reset=reset)
        for cycle in xrange(1, cycles+1):
            if cycles > 1:
                print ">>> update cycle", cycle
            self.fill(i=1, num=num)
            _clean_docs(db=self.rdb, srv=self.repsrv, prefix=self.prefix)
            self.replicate_1_to_n(n=n, normal=True)
            self._wait_till_all_equal(sources=1, targets=n, log=True)
            _wait_to_complete(rdb = self.rdb, prefix=self.prefix)


    def replicate_n_to_n_continuous_and_wait_till_equal(self, n=1, cycles=1, num=1, reset=False):
        self.create_dbs(n, n, reset=reset)
        _clean_docs(db=self.rdb, srv=self.repsrv, prefix=self.prefix)
        self.replicate_n_to_n(n=n, normal=False)
        for cycle in xrange(cycles):
            if cycles > 1:
                print ">>> update cycle", cycle
            for src in xrange(1, n+1):
                self.fill(src, num=num)
            self._wait_till_all_equal(sources=n, targets=n, log=True)


    def replicate_n_to_n_normal_and_wait_till_equal(self, n=1, cycles=1, num=1, reset=False):
        self.create_dbs(n, n, reset=reset)
        for cycle in xrange(1, cycles+1):
            if cycles > 1:
                print ">>> update cycle", cycle
            for src in xrange(1, n+1):
                self.fill(src, num=num)
            _clean_docs(db=self.rdb, srv=self.repsrv, prefix=self.prefix)
            self.replicate_n_to_n(n=n, normal=True)
            self._wait_till_all_equal(sources=1, targets=n, log=True)
            _wait_to_complete(rdb = self.rdb, prefix=self.prefix)

    def replicate_n_chain_normal(self, n=1, cycles=1, num=1, reset=False):
        self.create_dbs(1, n)
        
        


    # Private methods

    def _wait_till_all_equal(self, sources, targets, log=True):
        t0 = time.time()
        if sources == 1 and targets == 1:
            return self._wait_till_equal(sources, targets)
        elif sources == 1 and targets > 1:
            for i in xrange(1, targets+1):
                if log:
                    print " > waiting for target ",i
                self._wait_till_equal(sources, i)
        elif sources > 1 and targets == 1:
            return self._wait_till_equal(sources, targets)
        elif sources == targets:
            for i in xrange(1, sources+1):
                if log:
                    print " > waiting for source-target pair",i
                self._wait_till_equal(i, i)
        if log:
            dt = time.time() - t0
            print " > changes propagated in %.3f sec" % dt


    def _wait_till_equal(self, src_id, target_id):
        _wait_till_dbs_equal(self.srcdb(src_id), self.tgtdb(target_id), self.fast_comparison)


    def _create_n_src_dbs(self, n, reset=None, filt=None):
        _create_n_dbs(n, prefix=self.prefix+'_src', reset=reset, srv=self.srcsrv, filt=filt)


    def _create_n_tgt_dbs(self, n, reset=None):
        _create_n_dbs(n, prefix=self.prefix+'_tgt', reset=reset, srv=self.tgtsrv)


    def _repdoc(self, src, tgt, filt, params):
        did = self.prefix + '_%07d_%07d' % (src, tgt)
        src_dbname = _remote_url(self.srcsrv, _src_dbname(src, self.prefix))
        tgt_dbname = _remote_url(self.tgtsrv, _tgt_dbname(tgt, self.prefix))
        doc = self.rep_params.copy()
        doc.update(params)
        doc.update(_id=did, source=src_dbname, target=tgt_dbname)
        if filt:
            doc['filter'] = '%s/%s' % (FILTER_DOC, FILTER_NAME)
        return doc


def getsrv(srv=None, timeout=0):
    """
    Get a couchdb.Server() instances. This can usually be passed to all
    subsequent commands.
    """
    if isinstance(srv, couchdb.Server):
        return srv
    elif srv is None:
        cfg = _getcfg()
        return couchdb.Server(cfg.server_url)
    elif isinstance(srv, basestring):
        if timeout > 0:
            sess = couchdb.Session(timeout=timeout)
            return couchdb.Server(url=srv, session=sess, full_commit=False)
        else:
            return couchdb.Server(url=srv, full_commit=False)


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
        db = "_replicator"
    return getdb(db, srv=srv, create=create, reset=reset)


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
    print "  * rep.replicate_n_to_n(20) # replicate 20 sources to 20 targets, 1->1, 2->2, etc"
    print "  * rep.getsrv() # get a CouchDB Server instance"
    print "  * rep.replicate_1_to_n_and_wait_till_equal(10, cycles=3, num=10)"
    print "    # replicate 1 source to 10 target"
    print "    # then update source with 10 docs and wait till they appear on target"
    print "    # do that for 3 cycles in a row."
    print
    import IPython
    auto_imports = "import rep; from rep import getsrv, getdb, getrdb; import couchdb"
    IPython.start_ipython(argv=["-c","'%s'" % auto_imports, "-i"])


class RetryTimeoutExceeded(Exception):
    """Exceed retry timeout"""


def _retry(check=bool, timeout=None, dt=1, log=True):
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
    def deco(f):
        def retry(*args, **kw):
            t0 = time.time()
            tf = t0 + timeout if timeout > 0 else None
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


def _clean_dbs(prefix, srv):
    srv = getsrv(srv)
    cnt = 0
    for dbname in srv:
        if dbname.startswith(prefix):
            del srv[dbname]
            print "deleted db",dbname
            cnt += 1
    return cnt



def _updocs(db, num, prefix, **kw):
    """
    Update a set of docs in a database using an incremental
    scheme with a prefix.
    """
    start, end = 1, num
    start_did = prefix + '_%07d' % start
    end_did = prefix + '_%07d' % end
    _clean_docs(prefix=prefix, db=db, startkey=start_did, endkey=end_did)
    def dociter():
        for i in range(start, end+1):
            doc = copy.deepcopy(kw)
            doc['_id'] = prefix + '%07d' % i
            yield doc
    for res in _bulk_updater(db, dociter):
        if res[0]:
            pass
        else:
            print " > ERROR:", db.name, res[1], res[2]


def _yield_revs(db, prefix=None, all_docs_params=None, batchsize=5000):
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
    return prefix + '_%07d' % num


def _src_dbname(num, prefix):
    if num is None:
        return prefix+'_src'
    return _dbname(num, prefix+'_src')


def _tgt_dbname(num, prefix):
    if num is None:
        return prefix+'_tgt'
    return _dbname(num, prefix+'_tgt')


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
    existing_dbs = set(srv)
    want_dbs = set((_dbname(i, prefix) for i in xrange(1, n+1)))
    if reset:
        found_dbs = list(want_dbs & existing_dbs)
        found_dbs.sort()
        for dbname in found_dbs:
            print "deleting", dbname
            del srv[dbname]
        missing_dbs = want_dbs
    else:
        missing_dbs = want_dbs - existing_dbs
    missing_list = list(missing_dbs)
    missing_list.sort()
    print "Creating",len(missing_list),"databases"
    t0 = time.time()
    for dbname in missing_list:
        db = srv.create(dbname)
        print "  > created",dbname
        _maybe_add_filter(db=db, ddoc_id=ddoc_id, filtstr=filt)
    dt = time.time() - t0
    print "Finished creating", len(missing_list), "dbs in %.3f sec" % dt


def _maybe_add_filter(db, ddoc_id, filtstr):
    if not filtstr:
        return
    if ddoc_id in db:
        rev = oldd['_rev']
        filt2 = copy.deepcopy(filt)
        filt2['_rev'] = rev
        db[ddoc_id] = filt2
    else:
        db[ddoc_id] = copy.deepcopy(filt)


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


@_retry(lambda x : x == {}, 24*3600, 10)
def _wait_to_complete(rdb, prefix):
    return _get_incomplete(rdb=rdb, prefix=prefix)


def _compare(db1, db2, srv=None, prefix=None, fast=False):
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
    if fast or sdiff12 or sdiff21:
        return len(sdiff12), len(sdiff21)
    # do the slow thing, this builds all docs in memory so don't use on large dbs
    dict1, dict2 = {}, {}
    sdocdiff12, sdocdiff21 = set(), set()
    for d1doc in _yield_docs(db1, prefix=prefix):
        d1doc.pop('_rev')
        dict1[d1doc['_id']] = d1doc
    for d2doc in _yield_docs(db2, prefix=prefix):
        d2doc.pop('_rev')
        _id = d2doc['_id']
        dict2[_id] = d2doc
        if _id not in dict1 or d2doc!=dict1[_id]:
            sdocdiff21.add(_id)
    for _id, d1doc in dict1.iteritems():
        if _id not in dict2 or d1doc != dict2[_id]:
            sdocdiff12.add(_id)
    return len(sdocdiff12), len(sdocdiff21)


@_retry((0,0), 24*3600, 10)
def _wait_till_dbs_equal(db1, db2, srv=None, prefix=None, fast=False):
    return _compare(db1, db2, srv=None, prefix=None, fast=fast)


def _2bool(v):
    if isinstance(v, bool):
        return v
    elif isinstance(v, basestring):
        if v.strip().lower() == 'true':
            return True
        return False
    else:
        raise ValueError("Invalid boolean value: %s" % v)


def _getcfg():
    p = configargparse.ArgParser(default_config_files=CFG_FILES)
    for (name, dflt, ev, hs) in CFG_DEFAULTS:
        aname = '--' +name
        if dflt is False:
            p.add_argument(aname, default=dflt, action="store_true", env_var=ev, help=hs)
        else:
            p.add_argument(aname, default=dflt, env_var=ev, help=hs)
    return p.parse_args()
