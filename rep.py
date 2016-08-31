import time
import copy
import uuid
import couchdb
from itertools import izip
import configargparse


# Specify list of files where to look for config options to load
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

# Retry times scheduled passed to CouchDB driver to use
# in case of connection failures
RETRY_DELAYS = [3, 10, 20, 30, 90, 180]


# Export a few top level functions directly so can use them at module level
# without having to build a Rep class instance.

def replicate_1_to_n_and_compare(*args, **kw):
    """(See Rep.replicate_1_to_n_and_compare)"""
    r = Rep(cfg=kw.pop('cfg', None))
    return r.replicate_1_to_n_and_compare(*args, **kw)


def replicate_n_to_1_and_compare(*args, **kw):
    """(See Rep.replicate_n_to_1_and_compare)"""
    r = Rep(cfg=kw.pop('cfg', None))
    return r.replicate_n_to_1_and_compare(*args, **kw)


def replicate_n_to_n_and_compare(*args, **kw):
    """(see doc of Rep.replicate_n_to_n_and_compare)"""
    r = Rep(cfg=kw.pop('cfg', None))
    return r.replicate_n_to_n_and_compare(*args, **kw)


def replicate_n_chain_and_compare(*args, **kw):
    """(See doc of Rep.replicate_n_chain_and_compare)"""
    r = Rep(cfg=kw.pop('cfg', None))
    return r.replicate_n_chain_and_compare(*args, **kw)


def replicate_all_and_compare(*args, **kw):
    """(See doc of Rep.replicate_all_and_compare)"""
    r = Rep(cfg=kw.pop('cfg', None))
    return r.replicate_all_and_compare(*args, **kw)


def clean(*args, **kw):
    """(See doc of Rep.clean)"""
    r = Rep(cfg=kw.pop('cfg', None))
    r.clean(*args, **kw)
    return r


def srcdocs(*args, **kw):
    """(See doc of Rep.srcdocs)"""
    r = Rep(cfg=kw.pop('cfg', None))
    return r.srcdocs(*args)


def tgtdocs(*args, **kw):
    """(See doc of Rep.tgtdocs)"""
    r = Rep(cfg=kw.pop('cfg', None))
    return r.tgtdocs(*args)


def repdocs(*args, **kw):
    """(See doc of Rep.repdocs)"""
    r = Rep(cfg=kw.pop('cfg', None))
    return r.repdocs(*args)


def srcdb(*args, **kw):
    """(See doc of Rep.srcdb)"""
    r = Rep(cfg=kw.pop('cfg', None))
    return r.srcdb(*args)


def tgtdb(*args, **kw):
    """(See tgtdb of Rep.tgtdb)"""
    r = Rep(cfg=kw.pop('cfg', None))
    return r.tgtdb(*args)


def getcfg():
    """
    Return configuration object from configparse module.
    This object contains configuration gathered from multiple sources:
     * defaults in this module
     * config files
     * environment variables
     * command-line arguments
    """
    p = configargparse.ArgParser(default_config_files=CFG_FILES)
    for (name, dflt, ev, hs) in CFG_DEFAULTS:
        aname = '--' + name
        if dflt is False:
            p.add_argument(aname, default=dflt, action="store_true", env_var=ev, help=hs)
        else:
            p.add_argument(aname, default=dflt, env_var=ev, help=hs)
    return p.parse_known_args()[0]


def getsrv(srv=None, timeout=0):
    """
    Get a couchdb.Server() instances. This can usually be passed to all
    subsequent commands. It can be used to inspect all available databases.

    Example:
      In [0]: srv = rep.getsrv()
      In [1]: list(srv)
      Out[2]:
        [u'_replicator', u'rdyno_0000001', u'rdyno_0000002', u'rdyno_0000003']
    """
    if isinstance(srv, couchdb.Server):
        return srv
    elif srv is None:
        cfg = getcfg()
        sess = couchdb.Session(retry_delays=RETRY_DELAYS)
        return couchdb.Server(cfg.server_url, session=sess)
    elif isinstance(srv, basestring):
        if timeout > 0:
            sess = couchdb.Session(timeout=timeout, retry_delays=RETRY_DELAYS)
            return couchdb.Server(url=srv, session=sess)
        else:
            sess = couchdb.Session(retry_delays=RETRY_DELAYS)
            return couchdb.Server(url=srv, session=sess)


def getdb(db, srv=None, create=True, reset=False):
    """
    Get a couchdb.Database() instance. This can be used to manipulate
    documents in a database.

    Example:
      In [0]: d1 = rep.getdb('rdyno_0000001')
      In [1]: list(d1)
      Out[2]: [u'_design/rdyno_viewdoc', u'rdyno_0000001']
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
            print "got resource not found on create", dbname, "retrying..."
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
    Get replication couchdb.Database() instance. By default this is the
    "_replicator" database.

    Example:
       In [0]: rdb = rep.getrdb()
       In [1]: list(rdb)
       Out[2]: [u'_design/_replicator', u'rdyno_0000001_0000002']
    """
    if db is None:
        db = "_replicator"
    return getdb(db, srv=srv, create=create, reset=reset)


class Rep(object):
    """
    Rep class instance holds configuration paramters for a replication test
    instance, such as prefix to use (so can use mulitple concurrenlty on same
    server, replication params like timeouts).

    A Rep class instance is configured from a configparse.ArgParser.parse_args()
    result. If that is not passed in a default one will be used.
    """

    def __init__(self, cfg=None):
        if cfg is None:
            cfg = getcfg()
        print
        print "configuration:"
        for k, v in sorted(cfg._get_kwargs()):
            print "  ", k, "=", v
        print
        rep_params = {}
        rep_params['worker_processes'] = int(cfg.worker_processes)
        rep_params['connection_timeout'] = int(cfg.connection_timeout)
        rep_params['create_target'] = _2bool(cfg.create_target)
        rep_params['http_connections'] = int(cfg.http_connections)
        self.rep_params = rep_params
        self.src_params = {}
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
        """
        Return source db object with a given numerical index.
        Example:
        >>> rep.srcdb(1)
        >>> <Database 'rdyno_0000001'>
        """
        return getdb(_dbname(i, self.prefix), srv=self.srcsrv)

    def tgtdb(self, i=1):
        """
        Return target db object with a given numerical index.
        Example:
        >>> rep.tgtdb(2)
        >>> <Database 'rdyno_0000002'>
        """
        return getdb(_dbname(i, self.prefix), srv=self.tgtsrv)

    def srcdocs(self, i=1):
        """
        Return a list of all documents from a source db. Db is specified as
        a numerical index (1,2, ...)
        """
        res = []
        db = self.srcdb(i=i)
        for did in db:
            res += [dict(db[did])]
        return res

    def tgtdocs(self, i=1):
        """
        Return a list of all documents from a target db. Db is specified as
        a numerical index (1,2, ...)
        """
        res = []
        db = self.tgtdb(i=i)
        for did in db:
            res += [dict(db[did])]
        return res

    def repdocs(self):
        """
        Return a list of all the replication documents in the default
        replication db.
        """
        res = []
        db = self.rdb
        for did in db:
            if '_design' in did:
                res += [{'_id': did}]
                continue
            res += [dict(db[did])]
        return res

    def fill(self, i=1, num=1, rand_ids=False, src_params=None, attachments=None):
        """
        Fill a source db (specified as a numerical index) with num (=1) documents.

        :param i: Db index
        :param num: How many documents to write
        :param rand_ids: Whether to use random ids or not
        :param src_params: Optional dict of extra parameters to add. Can use
          this to generate huge documents or to specify fields to filter on.
        :param attachments: Add attachments to each document. Default is None
        :type attachments: Flexible type, can be specified as:
             {'name' : 'contents', ...}
          or [('name','contents'), ...]
          or 'contents' which is equivalent to [('att1', 'contents')]
          or int which is equivalent to [('att1', 'x'*int)]
        """
        if src_params is None:
            src_params = {}
        extra_data = copy.deepcopy(self.src_params)
        if src_params:
            extra_data.update(copy.deepcopy(src_params))

        db = self.srcdb(i=i)
        extra_data['some_data'] = uuid.uuid4().hex
        return _updocs(
            db=db,
            num=num,
            prefix=self.prefix,
            rand_ids=rand_ids,
            attachments=attachments,
            extra_data=extra_data)

    def updoc(self, db, doc):
        """
        Update a single doc in a database. This is used mainly to sync design documents.
        doc can be any dictionary. If doc exists then its '_rev' is read and used.
        If doc is None then nothing happens.
        """
        if doc is None:
            return
        _id = doc['_id']
        doc_copy = doc.copy()
        if _id in db:
            existing_doc = db[_id]
            doc_copy['_rev'] = existing_doc['_rev']
        db[_id] = doc_copy

    def clean(self):
        """
        Remove replication documents from default replication db and clean all dbs with
        configured prefix
        """
        _clean_docs(prefix=self.prefix, db=self.rdb)
        _clean_dbs(preifx=self.prefix+'-', srv=self.rdbsrv)
        _clean_dbs(prefix=self.prefix+'_', srv=self.srcsrv)
        _clean_dbs(prefix=self.prefix+'_', srv=self.tgtsrv)

    def create_dbs(self, source_range, target_range, reset=False):
        """
        Ensure db in source_range and target_range exist. Both source and target
        range are specifed as (low, high) numeric interval. However if
        `create_target` replication parameter is specified, target dbs are not created.
        If reset is True then databases are deleted, then re-created.
        """
        self._create_range_dbs(srv=self.srcsrv, numrange=source_range, reset=reset)
        if target_range and not self.rep_params.get('create_target'):
            self._create_range_dbs(srv=self.tgtsrv, numrange=target_range, reset=reset)

    def sync_filter(self, filter_ddoc, sr):
        """
        Given a filter design document and a range of source databases, make sure
        filter document is written to all those databases.
        """
        lo, hi = _db_range_validate(sr)
        for i in xrange(lo, hi+1):
            self.updoc(self.srcdb(i), filter_ddoc)

    def replicate_n_to_n(self, sr, tr, normal=False, db_per_doc=False, rep_params=None):
        """
        Generate "n-to-n" pattern replications.

        Example: if n is 3 generates:
          1 -> 4, 2 -> 5, 3 -> 6

        This method assumes databases have already been created.

        :param sr: Source db range specified as (low, high) interval.
        :param tr: Target db range specified as (low, high) interval. Source and target
          db interval sizes must be equal.
        :param normal: if `True` create a normal replication instead of continuous
        :param db_per_doc: if `True` create a separate replication db per each
          replication document.
        :param rep_params: additional replication parameters (usually filters)
        """
        if rep_params is None:
            rep_params = {}
        params = self.rep_params.copy()
        params.update(rep_params)
        params['continuous'] = not normal
        ipairs = izip(_xrange(sr), _xrange(tr))

        def dociter():
            for s, t in ipairs:
                yield self._repdoc(s, t, params=params)

        return _rdb_updater(self.repsrv, self.rdb, self.prefix, dociter, db_per_doc)

    def replicate_1_to_n(self, sr, tr, normal=False, db_per_doc=False, rep_params=None):
        """
        Generate "1-to-n" pattern replications.

        Example: if n is 3 then generates:
          1 -> 2, 1 -> 3,  1 -> 4

        This method assumes databases have already been created.

        :param sr: Source db range specified as (low, high) interval.
        :param tr: Target db range specified as (low, high) interval. Source and target
          db interval sizes must be equal.
        :param normal: if `True` create a normal replication instead of continuous
        :param db_per_doc: if `True` create a separate replication db per each
          replication document.
        :param rep_params: additional replication parameters (usually filters)
        """
        if rep_params is None:
            rep_params = {}
        params = self.rep_params.copy()
        params.update(rep_params)
        params['continuous'] = not normal
        xrs, xrt = _xrange(sr), _xrange(tr)
        assert len(xrs) == 1
        assert len(xrt) >= 1
        s = xrs[0]

        def dociter():
            for t in xrt:
                yield self._repdoc(s, t, params=params)

        return _rdb_updater(self.repsrv, self.rdb, self.prefix, dociter, db_per_doc)

    def replicate_n_to_1(self, sr, tr, normal=False, db_per_doc=False, rep_params=None):
        """
        Generate "n-to-1" pattern replications.

        Example: if n is 3 then generates:
          2 -> 1, 3 -> 1, 4 -> 1

        This method assumes databases have already been created.

        :param sr: Source db range specified as (low, high) interval.
        :param tr: Target db range specified as (low, high) interval. Source and target
          db interval sizes must be equal.
        :param normal: if `True` create a normal replication instead of continuous
        :param db_per_doc: if `True` create a separate replication db per each
          replication document.
        :param rep_params: additional replication parameters (usually filters)
        """
        if rep_params is None:
            rep_params = {}
        params = self.rep_params.copy()
        params.update(rep_params)
        params['continuous'] = not normal
        xrs, xrt = _xrange(sr), _xrange(tr)
        assert len(xrs) >= 1
        assert len(xrt) == 1
        t = xrt[0]

        def dociter():
            for s in xrs:
                yield self._repdoc(s, t, params=params)

        return _rdb_updater(self.repsrv, self.rdb, self.prefix, dociter, db_per_doc)

    def replicate_n_chain(self, sr, tr, normal=False, db_per_doc=False, rep_params=None):
        """
        Generate a chain of n replications.

        Example: if n is 3 then generates:
          1 -> 2, 2 -> 3

        This method assumes databases have already been create.

        :param sr: Source db range specified as (low, high) interval.
        :param tr: Target db range specified as (low, high) interval. Source and target
          db interval sizes must be equal.
        :param normal: if `True` create a normal replication instead of continuous
        :param db_per_doc: if `True` create a separate replication db per each
          replication document.
        :param rep_params: additional replication parameters (usually filters)
        """
        if rep_params is None:
            rep_params = {}
        params = self.rep_params.copy()
        params.update(rep_params)
        params['continuous'] = not normal
        xrs = _xrange(sr)
        assert len(xrs) > 1
        assert tr == 0  # target not used

        def dociter():
            prev_s = None
            for i, s in enumerate(xrs):
                if i == 0:
                    prev_s = s
                    continue
                else:
                    yield self._repdoc(prev_s, s, params=params)
                    prev_s = s

        return _rdb_updater(self.repsrv, self.rdb, self.prefix, dociter, db_per_doc)

    def replicate_all(self, sr, tr, normal=False, db_per_doc=False, rep_params=None):
        """
        Generate a complete replication graph of n nodes. This method is used
        to generate a maximum number of replication based on a smaller number
        of databases. It generates a n^2 replication for n databases. For example,
        can create 10k replications from just 100 databases.

        Example: if n is 3 then generates:
          1 -> 1, 1 -> 2, 1 -> 3,
          2 -> 1, 2 -> 2, 2 -> 3
          3 -> 1, 3 -> 2, 3 -> 3

        This method assumes databases have already been created.

        :param sr: Source db range specified as (low, high) interval.
        :param tr: Target db range specified as (low, high) interval. Source and target
          db interval sizes must be equal.
        :param normal: if `True` create a normal replication instead of continuous
        :param db_per_doc: if `True` create a separate replication db per each
          replication document.
        :param rep_params: additional replication parameters (usually filters)
        """
        if rep_params is None:
            rep_params = {}
        params = self.rep_params.copy()
        params.update(rep_params)
        params['continuous'] = not normal
        assert tr == 0  # target not used
        xrs = _xrange(sr)
        assert len(xrs) >= 1

        def dociter():
            for s1 in xrs:
                for s2 in xrs:
                    yield self._repdoc(s1, s2, params=params)

        return _rdb_updater(self.repsrv, self.rdb, self.prefix, dociter, db_per_doc)

    def replicate_1_to_n_and_compare(self, n=1, cycles=1, num=1, normal=False,
                                     db_per_doc=False, rep_params=None, src_params=None,
                                     attachments=None, reset=False, filter_js=None,
                                     filter_mango=None, filter_doc_ids=None, filter_view=None,
                                     filter_query_params=None):
        """
        Create source and/or target databases. Fill source with data (num=1 docs).

        Generate 1-to-n replications (see replicate_1_to_n doc for details).

        Wait until changes from source db propagates to targets.

        Repeat `cycles` (=1) number of times.

        :param n: How many replications to create.
        :param cycles: How many times to repeat.
        :param num: How many documents to write to source.
        :param normal: If True use normal instead of continuous replications.
           (Normal replications delete and re-create replication docs each cycle).
        :param db_per_doc: If True, then create a replicator db per each document.
        :param rep_params: Additional parameters to write to replication docs.
        :param src_params: Additional parameters to write to source docs.
        :param attachments: Add optional attachment to _each_ doc in source db.
        :type attachments: Flexible type, can be specified as:
             {'name' : 'contents', ...}
          or [('name','contents'), ...]
          or 'contents' which is equivalent to [('att1', 'contents')]
          or int which is equivalent to [('att1', 'x'*int)]
        :param reset: If True, during setup, delete existing databases first
          (don't reuse)
        :param filter_js: True|str specify a user Javascript filter to use.
          Can be True to specify `function(doc, req) {return true;}`
        :param filter_mango: True|dict specify a Mango filter selector object.
          Can be True to specify `{"_id" : {"$ne" : None}}`
        :param filter_doc_ids: True|[str] specify a list of doc_ids to use as
          `doc_ids` filter. If True then filter is [self.prefix+'_0000001']
        :param filter_view: Specify a view filter map function, used for _view
          filter. If true then `function(doc) { emit(doc._id, null); };` is used.
        :param filter_query_params: Specify optional params for user JS filter.
        """
        sr, tr = 1, (2, n+1)
        repmeth = self.replicate_1_to_n
        filter_params = dict(js=filter_js, mango=filter_mango, doc_ids=filter_doc_ids,
                             view=filter_view, query_params=filter_query_params)

        def fillcb():
            self.fill(1, num=num, src_params=src_params, attachments=attachments)

        return self._setup_and_compare(normal, sr, tr, cycles, num, reset, repmeth,
                                       fillcb, db_per_doc, rep_params, filter_params)

    def replicate_n_to_1_and_compare(self, n=1, cycles=1, num=1, normal=False,
                                     db_per_doc=False, rep_params=None, src_params=None,
                                     attachments=None, reset=False, filter_js=None,
                                     filter_mango=None, filter_doc_ids=None, filter_view=None,
                                     filter_query_params=None):
        """
        Create source and/or target databases. Fill source with data (num=1 docs).

        Generate n-to-1 replications (see replicate_n_to_1 doc for details).

        Wait until changes from source dbs propagates to target. Unlike other
        replications it fills sources dbs with random ids (so they don't collide on
        the target).

        Repeat `cycles` (=1) number of times.

        :param n: How many replications to create.
        :param cycles: How many times to repeat.
        :param num: How many documents to write to source.
        :param normal: If True use normal instead of continuous replications.
           (Normal replications delete and re-create replication docs each cycle).
        :param db_per_doc: If True, then create a replicator db per each document.
        :param rep_params: Additional parameters to write to replication docs.
        :param src_params: Additional parameters to write to source docs.
        :param attachments: Add optional attachment to _each_ doc in source db.
        :type attachments: Flexible type, can be specified as:
             {'name' : 'contents', ...}
          or [('name','contents'), ...]
          or 'contents' which is equivalent to [('att1', 'contents')]
          or int which is equivalent to [('att1', 'x'*int)]
        :param reset: If True, during setup, delete existing databases first
          (don't reuse)
        :param filter_js: True|str specify a user Javascript filter to use.
          Can be True to specify `function(doc, req) {return true;}`
        :param filter_mango: True|dict specify a Mango filter selector object.
          Can be True to specify `{"_id" : {"$ne" : None}}`
        :param filter_doc_ids: True|[str] specify a list of doc_ids to use as
          `doc_ids` filter. If True then filter is [self.prefix+'_0000001']
        :param filter_view: Specify a view filter map function, used for _view
          filter. If true then `function(doc) { emit(doc._id, null); };` is used.
        :param filter_query_params: Specify optional params for user JS filter.
        """
        sr, tr = (2, n+1), 1
        repmeth = self.replicate_n_to_1
        filter_params = dict(js=filter_js, mango=filter_mango, doc_ids=filter_doc_ids,
                             view=filter_view, query_params=filter_query_params)

        def fillcb():
            for src in _xrange(sr):
                self.fill(src, num=num, rand_ids=True,  src_params=src_params,
                          attachments=attachments)

        return self._setup_and_compare(normal, sr, tr, cycles, num, reset, repmeth,
                                       fillcb, db_per_doc, rep_params, filter_params)

    def replicate_n_to_n_and_compare(self, n=1, cycles=1, num=1, normal=False,
                                     db_per_doc=False, rep_params=None, src_params=None,
                                     attachments=None, reset=False, filter_js=None,
                                     filter_mango=None, filter_doc_ids=None, filter_view=None,
                                     filter_query_params=None):
        """
        Create source and/or target databases. Fill source with data (num=1 docs).

        Generate n-to-n replications (see replicate_n_to_n doc for details).
        It creates n database pairs (so 2*n total databases).

        Wait until changes from source dbs propagates to targets.

        Repeat `cycles` (=1) number of times.

        :param n: How many replications to create.
        :param cycles: How many times to repeat.
        :param num: How many documents to write to source.
        :param normal: If True use normal instead of continuous replications.
          (Normal replications delete and re-create replication docs each cycle).
        :param db_per_doc: If True, then create a replicator db per each document.
        :param rep_params: Additional parameters to write to replication docs.
        :param src_params: Additional parameters to write to source docs.
        :param attachments: Add optional attachment to _each_ doc in source db.
        :type attachments: Flexible type, can be specified as:
             {'name' : 'contents', ...}
          or [('name','contents'), ...]
          or 'contents' which is equivalent to [('att1', 'contents')]
          or int which is equivalent to [('att1', 'x'*int)]
        :param reset: If True, during setup, delete existing databases first
          (don't reuse)
        :param filter_js: True|str specify a user Javascript filter to use.
          Can be True to specify `function(doc, req) {return true;}`
        :param filter_mango: True|dict specify a Mango filter selector object.
          Can be True to specify `{"_id" : {"$ne" : None}}`
        :param filter_doc_ids: True|[str] specify a list of doc_ids to use as
          `doc_ids` filter. If True then filter is [self.prefix+'_0000001']
        :param filter_view: Specify a view filter map function, used for _view
          filter. If true then `function(doc) { emit(doc._id, null); };` is used.
        :param filter_query_params: Specify optional params for user JS filter.
        """
        sr, tr = (1, n), (n+1, 2*n)
        repmeth = self.replicate_n_to_n
        filter_params = dict(js=filter_js, mango=filter_mango, doc_ids=filter_doc_ids,
                             view=filter_view, query_params=filter_query_params)

        def fillcb():
            for src in _xrange(sr):
                self.fill(src, num=num, src_params=src_params, attachments=attachments)

        return self._setup_and_compare(normal, sr, tr, cycles, num, reset, repmeth,
                                       fillcb, db_per_doc, rep_params, filter_params)

    def replicate_n_chain_and_compare(self, n=2, cycles=1, num=1, normal=False,
                                      db_per_doc=False, rep_params=None, src_params=None,
                                      attachments=None, reset=False, filter_js=None,
                                      filter_mango=None, filter_doc_ids=None, filter_view=None,
                                      filter_query_params=None):
        """
        Create source and/or target databases. Fill source with data (num=1 docs).

        Generate chain of n replication (see replicate_n_chain doc for details).

        Wait until changes from source db propagates to all targets.

        Repeat `cycles` (=1) number of times.

        :param n: How many replications to create.
        :param cycles: How many times to repeat.
        :param num: How many documents to write to source.
        :param normal: If True use normal instead of continuous replications.
           (Normal replications delete and re-create replication docs each cycle).
        :param db_per_doc: If True, then create a replicator db per each document.
        :param rep_params: Additional parameters to write to replication docs.
        :param src_params: Additional parameters to write to source docs.
        :param attachments: Add optional attachment to _each_ doc in source db.
        :type attachments: Flexible type, can be specified as:
             {'name' : 'contents', ...}
          or [('name','contents'), ...]
          or 'contents' which is equivalent to [('att1', 'contents')]
          or int which is equivalent to [('att1', 'x'*int)]
        :param reset: If True, during setup, delete existing databases first
          (don't reuse)
        :param filter_js: True|str specify a user Javascript filter to use.
          Can be True to specify `function(doc, req) {return true;}`
        :param filter_mango: True|dict specify a Mango filter selector object.
          Can be True to specify `{"_id" : {"$ne" : None}}`
        :param filter_doc_ids: True|[str] specify a list of doc_ids to use as
          `doc_ids` filter. If True then filter is [self.prefix+'_0000001']
        :param filter_view: Specify a view filter map function, used for _view
          filter. If true then `function(doc) { emit(doc._id, null); };` is used.
        :param filter_query_params: Specify optional params for user JS filter.
        """
        if n < 2:
            raise ValueError("A chain requires a minimim of 2 nodes")
        sr, tr = (1, n), 0  # target not used here, only sources
        repmeth = self.replicate_n_chain
        filter_params = dict(js=filter_js, mango=filter_mango, doc_ids=filter_doc_ids,
                             view=filter_view, query_params=filter_query_params)

        def fillcb():
            self.fill(1, num=num, src_params=src_params, attachments=attachments)

        return self._setup_and_compare(normal, sr, tr, cycles, num, reset, repmeth,
                                       fillcb, db_per_doc, rep_params, filter_params)

    def replicate_all_and_compare(self, n=1, cycles=1, num=1, normal=False,
                                  db_per_doc=False, rep_params=None, src_params=None,
                                  attachments=None, reset=False, filter_js=None,
                                  filter_mango=None, filter_doc_ids=None, filter_view=None,
                                  filter_query_params=None):
        """
        Create source and/or target databases. Fill source with data (num=1 docs).

        Generate  all n to all n replications (see replicate_all doc for details).
        This generates n*2 replications from only n databases.

        Wait until changes from source db propagates to targets.

        Repeat `cycles` (=1) number of times.

        :param n: How many replications to create.
        :param cycles: How many times to repeat.
        :param num: How many documents to write to source.
        :param normal: If True use normal instead of continuous replications.
          (Normal replications delete and re-create replication docs each cycle).
        :param db_per_doc: If True, then create a replicator db per each document.
        :param rep_params: Additional parameters to write to replication docs.
        :param src_params: Additional parameters to write to source docs.
        :param attachments: Add optional attachment to _each_ doc in source db.
        :type attachments: Flexible type, can be specified as:
             {'name' : 'contents', ...}
          or [('name','contents'), ...]
          or 'contents' which is equivalent to [('att1', 'contents')]
          or int which is equivalent to [('att1', 'x'*int)]
        :param reset: If True, during setup, delete existing databases first
          (don't reuse)
        :param filter_js: True|str specify a user Javascript filter to use.
          Can be True to specify `function(doc, req) {return true;}`
        :param filter_mango: True|dict specify a Mango filter selector object.
          Can be True to specify `{"_id" : {"$ne" : None}}`
        :param filter_doc_ids: True|[str] specify a list of doc_ids to use as
          `doc_ids` filter. If True then filter is [self.prefix+'_0000001']
        :param filter_view: Specify a view filter map function, used for _view
          filter. If true then `function(doc) { emit(doc._id, null); };` is used.
        :param filter_query_params: Specify optional params for user JS filter.
        """
        sr, tr = (1, n), 0  # target not used here, only sources
        repmeth = self.replicate_all
        filter_params = dict(js=filter_js, mango=filter_mango, doc_ids=filter_doc_ids,
                             view=filter_view, query_params=filter_query_params)

        def fillcb():
            self.fill(1, num=num, src_params=src_params, attachments=attachments)

        return self._setup_and_compare(normal, sr, tr, cycles, num, reset, repmeth,
                                       fillcb, db_per_doc, rep_params, filter_params)

    # Private methods

    def _filter_ddoc_and_rep_params(self, filter_params, rep_params):
        """
        Take a dictionary of filter_params = {'js':'function...', } and
        rep_params (which could be None), parse filter_params and generate
        a filter design doc (which could be None) and a merged rep_params
        """
        params = filter_params.copy()
        query_params = params.pop('query_params', None)
        assert set(params.keys()) == set(['js', 'mango', 'view', 'doc_ids'])
        specified = dict([(k, v) for (k, v) in params.items() if v is not None])
        if not specified:
            return (None, rep_params)
        if len(specified) > 1:
            raise ValueError("Only 1 filter can be specified %s" % specified)
        fname, fbody = specified.popitem()
        ddoc = None
        if fname is 'js':
            ddoc, rep_params_up = self._filter_js(fbody, query_params=query_params)
        if fname is 'mango':
            ddoc, rep_params_up = self._filter_mango(fbody)
        if fname is 'doc_ids':
            ddoc, rep_params_up = self._filter_doc_ids(fbody)
        if fname is 'view':
            ddoc, rep_params_up = self._filter_view(fbody)
        if rep_params is not None:
            rep_params_up.update(rep_params)
        return (ddoc, rep_params_up)

    def _filter_js(self, filter_body, query_params=None):
        """
        Process a Javascript filter. Accept filter_body as a string or
        or True. If True use a default pass-all filter.
        """
        if filter_body is True:
            filter_body = "function(doc, req) {return true;}"
        assert isinstance(filter_body, basestring), "Filter body must a string"
        filter_doc = '%s_filterdoc' % self.prefix
        filter_name = '%s_filtername' % self.prefix
        ddoc = {
            "_id": '_design/%s' % filter_doc,
            "filters": {
                filter_name: filter_body
            }
        }
        rep_params = {
            "filter": "%s/%s" % (filter_doc, filter_name)
        }
        if query_params:
            rep_params['query_params'] = query_params
        return (ddoc, rep_params)

    def _filter_mango(self, selector_obj):
        """
        Process a Mango selector filter. selector_obj should be either a Mango
        selector object or True. If True then default pass-through selector
        will be used.
        """
        if selector_obj is True:
            selector_obj = {"_id": {"$ne": None}}
        assert isinstance(selector_obj, dict)
        return (None, {"selector": selector_obj})

    def _filter_doc_ids(self, doc_ids):
        """
        Process a doc_ids filter. doc_ids should be a list of doc_ids or True.
        If True, then generates a list of 1 document, which is the default
        1st document generated by default source fill algorithm.
        """
        if doc_ids is True:
            doc_ids = ['%s_%07d' % (self.prefix, 1)]
        assert isinstance(doc_ids, list)
        return (None, {"doc_ids": doc_ids})

    def _filter_view(self, map_body):
        """
        Process a _view filter. map_body should be a string representing a
        view map function or True. If True then a pass-through map function
        will be used (it emits all seen documents ids).
        """
        if map_body is True:
            map_body = "function(doc) { emit(doc._id, null); };"
        assert isinstance(map_body, basestring), "View map must be a string"
        view_doc = '%s_viewdoc' % self.prefix
        view_name = '%s_viewname' % self.prefix
        ddoc = {
            "_id": "_design/%s" % view_doc,
            "views": {
                view_name: {
                    "map": map_body
                }
            }
        }
        rep_params = {
            "filter": "_view",
            "query_params": {"view": "%s/%s" % (view_doc, view_name)}
        }
        return (ddoc, rep_params)

    def _setup_and_compare(self, normal, sr, tr, cycles, num, reset, rep_method,
                           fill_callback, db_per_doc, rep_params, filter_params):
        """
        Common utility method for all replicate_*_and_compare functions.

        Sets up databases. Parses filters and puts filter design docs (if any) to
        source dbs.

        If replications are `normal` then each cycle:
          - fill sources with data
          - create replications
          - wait till changes propagate to target

        If replications are continuous:
          - fill sources with data
          - wait till changes propagate to target

        Parameters can configure, source and target db ranges as tuples of (low, high),
        number of docs to write to soruce, replicaton callback method to use. Source
        fill callback method to use. Whether to use a single replicator db per each doc,
        additional replication and filter params.
        """
        filter_ddoc, rep_params = self._filter_ddoc_and_rep_params(filter_params, rep_params)
        self._clean_rep_docs(db_per_doc)
        self.create_dbs(sr, tr, reset=reset)
        self.sync_filter(filter_ddoc, sr)
        if normal:
            for cycle in xrange(1, cycles+1):
                if cycles > 1:
                    print
                    print ">>>>>> cycle", cycle, "<<<<<<"
                fill_callback()
                self._clean_rep_docs(db_per_doc)
                rep_method(sr, tr, normal=True, db_per_doc=db_per_doc, rep_params=rep_params)
                time.sleep(1)
                self._wait_till_all_equal(sr, tr, log=True)
                if not db_per_doc:
                    _wait_to_complete(rdb=self.rdb, prefix=self.prefix)
        else:
            rep_method(sr, tr, normal=False, db_per_doc=db_per_doc, rep_params=rep_params)
            for cycle in xrange(1, cycles+1):
                if cycles > 1:
                    print
                    print ">>>>>> cycle", cycle, "<<<<<<"
                fill_callback()
                time.sleep(1)
                self._wait_till_all_equal(sr, tr, log=True)
        return self

    def _wait_till_all_equal(self, sr, tr, log=True):
        """
        Compare soure(s) and target(s) dbs for equality
        """
        t0 = time.time()
        xrs, xrt = _xrange(sr), _xrange(tr)
        if len(xrt) == 0:
            if len(xrs) <= 1:
                return
            prev_s = xrs[0]
            for (i, s) in enumerate(xrs):
                if i == 0:
                    prev_s = s
                    continue
                if log:
                    print " > comparing source", prev_s, s
                _wait_to_propagate(self.srcdb(prev_s), self.srcdb(s), self.prefix)
                prev_s = s
        elif len(xrs) == 1 and len(xrt) == 1:
            print " > checking if target and source equal", xrs[0], xrt[0]
            _wait_to_propagate(self.srcdb(xrs[0]), self.tgtdb(xrt[0]), self.prefix)
        elif len(xrs) == 1 and len(xrt) > 1:
            s = xrs[0]
            for t in xrt:
                if log:
                    print " > comparing target ", t
                _wait_to_propagate(self.srcdb(s), self.tgtdb(t), self.prefix)
        elif len(xrs) > 1 and len(xrt) == 1:
            t = xrt[0]
            for s in xrs:
                if log:
                    print " > comparing source", s
                _wait_to_propagate(self.srcdb(s), self.tgtdb(t), self.prefix)
        elif len(xrt) == len(xrs):
            for (s, t) in izip(xrs, xrt):
                if log:
                    print " > comparing source-target pair", s, t
                _wait_to_propagate(self.srcdb(s), self.tgtdb(t), self.prefix)
        else:
            raise ValueError("Cannot compare arbitrary source and target dbs %s %s" % (sr, tr))
        if log:
            dt = time.time() - t0
            print " > changes propagated in at least %.1f sec" % dt

    def _create_range_dbs(self, srv, numrange, reset=None):
        lo, hi = _db_range_validate(numrange)
        _create_range_dbs(lo, hi, prefix=self.prefix, reset=reset, srv=srv)

    def _repdoc(self, src, tgt, params):
        """
        Generate a replication document from prefix, numeric indices and
        database URLs
        """
        did = self.prefix + '_%07d_%07d' % (src, tgt)
        src_dbname = _remote_url(self.srcsrv, _dbname(src, prefix=self.prefix))
        tgt_dbname = _remote_url(self.tgtsrv, _dbname(tgt, prefix=self.prefix))
        doc = self.rep_params.copy()
        doc.update(params)
        doc.update(_id=did, source=src_dbname, target=tgt_dbname)
        return doc

    def _clean_rep_docs(self, db_per_doc):
        prefix = self.prefix + '_repdb_'
        if db_per_doc:
            print " > cleaning up replicator dbs prefix:", prefix
            _clean_dbs(prefix=prefix, srv=self.repsrv)
        else:
            print " > cleaning existing docs from rep db:", self.rdb.name, "doc prefix:", self.prefix
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
    auto_imports = "import rep; from rep import getsrv, getdb, getrdb, Rep; import couchdb"
    IPython.start_ipython(argv=["-c", "'%s'" % auto_imports, "-i"])


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
    if dt <= 0:
        dt = 0.001

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
                except Exception, e:
                    fn = _fname(f)
                    print " > function", fn, "threw exception", e, "retrying"
                    t += 1
                    time.sleep(dt * 2**t)
                    continue
                t = 1
                if (callable(check) and check(r)) or check == r:
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
            print "   > deleted db:", dbname
            cnt += 1
    return cnt


def _db_range_validate(numrange):
    if isinstance(numrange, int) or isinstance(numrange, long):
        numrange = (numrange, numrange)
    assert isinstance(numrange, tuple)
    assert len(numrange) == 2
    lo, hi = numrange
    lo = int(lo)
    hi = int(hi)
    if lo > hi:
        lo = hi
    return lo, hi


def _xrange(r):
    if isinstance(r, int) or isinstance(r, long):
        if r <= 0:
            return ()
        r = (r, r)
    assert isinstance(r, tuple)
    assert len(r) == 2
    assert r[0] <= r[1]
    return xrange(r[0], r[1] + 1)


def _updocs(db, num, prefix, rand_ids, attachments, extra_data):
    """
    Update a set of docs in a database using an incremental
    scheme with a prefix.
    """
    start, end = 1, num
    _clean_docs(prefix=prefix, db=db, startkey=prefix+'_', endkey=prefix+'_zzz')
    conflicts = set()
    retry = True
    to_attach = {}

    while retry:

        def dociter():
            if conflicts:
                for _id in conflicts.pop():
                    doc = extra_data.copy()
                    yield doc
            else:
                for i in range(start, end+1):
                    _id = prefix + '_%07d' % i
                    doc = extra_data.copy()
                    if rand_ids:
                        _id += '_' + uuid.uuid4().hex
                        doc['_id'] = _id
                    yield doc

        for res in _bulk_updater(db, dociter):
            if res[0]:
                doc_id = res[1]
                doc_rev = res[2]
                print " > updated doc", db.name, doc_id, doc_rev
                if 'conflict' in doc_rev:
                    conflicts.add(doc_id)
                    continue
                if attachments:
                    to_attach[doc_id] = doc_rev
            else:
                print " > ERROR:", db.name, res[1], res[2]
        if conflicts:
            print " > WARNING: found", len(conflicts), " conflicts, retrying to add them"
        else:
            retry = False

    for _id, _rev in to_attach.iteritems():
        _put_attachments(db, _id, _rev, attachments)


def _put_attachments(db, doc_id, doc_rev, attachments):
    """
    Add attachments to a document. Attachments can be
    specified in various ways:

      - {'name' : 'contents', ...}
      - [('name','contents'), ...]
      - 'contents' which is equivalent to [('att1', 'contents')]
      - int which is equivalent to [('att1', 'x'*int)]
    """
    if attachments is None:
        attachments = []
    if isinstance(attachments, int) or isinstance(attachments, long):
        attachments = "x"*attachments
    if isinstance(attachments, basestring):
        attachments = [("att1", attachments)]
    if isinstance(attachments, dict):
        attachments = attachments.iteritems()
    doc = {'_id': doc_id, '_rev': doc_rev}
    for (name, val) in attachments:
        name_str = str(name)
        if isinstance(val, int) or isinstance(val, long):
            val_str = "x" * val
        else:
            val_str = str(val)
        print "   > put attachment:", db.name, doc, name_str, len(val_str)
        db.put_attachment(doc, val_str, filename=name_str)


def _yield_revs(db, prefix=None, all_docs_params=None, batchsize=500):
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


def _yield_docs(db, prefix=None, batchsize=500):
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
        batch = [x for (_, x) in izip(xrange(batchsize), it)]
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
    """
    Bulk updater for replication databases and docs. If
    instructed will do crazy things such as creating a
    separate database for each replcition document.
    """
    if db_per_doc:
        for n, doc in enumerate(dociter()):
            _rdb_and_doc(repsrv, prefix, n+1, doc)
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
        for _id, _rev in doc_revs:
            yield dict(_id=_id, _rev=_rev, _deleted=True)

    cnt = 0
    for res in _bulk_updater(db, dociter, batchsize=1000):
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


def _create_range_dbs(lo, hi, prefix, reset=False, srv=None):
    srv = getsrv(srv)
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
    print "Creating", len(missing_list), "databases"
    for dbname in missing_list:
        srv.create(dbname)
        print "  > created", dbname


def _remote_url(srv, dbname):
    if '://' in dbname:
        return dbname
    url = srv.resource.url
    usr, pwd = srv.resource.credentials
    schema, rest = url.split('://')
    return '://'.join([schema, '%s:%s@%s/%s' % (usr, pwd, rest, dbname)])


def _get_incomplete(rdb, prefix):
    res = {}
    for doc in _yield_docs(rdb, prefix=prefix):
        did = doc.get("_id")
        _replication_state = doc.get('_replication_state', '')
        if _replication_state == "completed":
            continue
        res[str(did)] = str(_replication_state)
    return res


@_retry(lambda x: x == {}, 24 * 3600, 10, False)
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
    s1 = set((_id[0] for _id in _yield_revs(db1, prefix=prefix, batchsize=500)))
    s2 = set((_id[0] for _id in _yield_revs(db2, prefix=prefix, batchsize=500)))
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
