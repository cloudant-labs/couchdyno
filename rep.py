#!/usr/bin/python

# Replication toolbox library to be used from ipython
# (or basic Python prompt).

import time, copy, uuid, couchdb  # pip : CouchDB==1.0

PREFIX='rdyno_'
SRC_PREFIX=PREFIX+'src_'
TGT_PREFIX=PREFIX+'tgt_'
PAYLOAD_PREFIX=PREFIX+'data_'
FILTER_DOC='rdynofilterdoc'
FILTER_NAME='rdynofilter'
TIMEOUT=300 # seconds

def getsrv(srv_or_port=None):
    if isinstance(srv_or_port, couchdb.Server):
        return srv_or_port
    port = srv_or_port
    if port is None:
        port = 15984
    sess = couchdb.Session(timeout=TIMEOUT)
    s = couchdb.Server('http://adm:pass@localhost:%s' % port, session=sess)
    s.version()
    return s


def getdb(dbname, srv=None, create=True, reset=False):
    if isinstance(dbname, couchdb.Database):
        dbname = dbname.name
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
    
def getrdb(srv=None, name='_replicator', create=True, reset=False):
    if name is None:
        name = '_replicator'
    return getdb(name, srv=srv, create=create, reset=reset)

def clean_dbs(prefix=PREFIX, srv=None):
    srv = getsrv(srv)
    cnt = 0
    for dbname in srv:
        if dbname.startswith(prefix):
            del srv[dbname]
            print "deleted db",dbname
            cnt += 1
    return cnt

def clean_docs(prefix, db, srv=None):
    db = getdb(db, srv=srv, create=False, reset=False)
    cnt = 0
    for did in [did for did in db if did.startswith(prefix)]:
        del db[did] # does the get doc, check rev, delete dance here
        print "deleted doc",did
        cnt += 1
    return cnt

def _dbname(num, prefix):
    return prefix + '%05d' % num

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

def rdoc(src, tgt, filt=None):
    doc = {'source': src, 'target':tgt}
    if filt:
        doc['filter'] = '%s/%s' % (FILTER_DOC, FILTER_NAME)
    doc['worker_processes']  = 4
    doc['connection_timeout'] = 120000
    doc['http_connections'] = 5
    doc['retries_per_request'] = 10
    doc['continuous'] = True
    return doc

def remote_url(srv, dbname):
    if '://' in dbname:
        return dbname
    url = srv.resource.url
    usr, pwd = srv.resource.credentials
    schema,rest = url.split('://')
    return '://'.join([schema, '%s:%s@%s/%s' % (usr, pwd, rest, dbname)])

def updoc(db, did, **kw):
    if did in db:
        old = db[did]
        old.update(**kw)
        db[did] = old
        return dict(old)
    else:
        doc = kw
        db[did] = doc
        return dict(doc)


def updocs(db,start=1,end=1,prefix=PAYLOAD_PREFIX,**kw):
    res = []
    for i in range(start, end+1):
        did = prefix+str(i)
        res += [updoc(db=db, did=did, **kw)]
    return res
            

def create_replication(src, tgt, reset=False, filt=None, rdb=None, srv=None):
    srv = getsrv(srv)
    rdb = getrdb(srv=srv, name=rdb)
    did = PREFIX + '%05d_%05d' % (src, tgt)
    src_dbname = remote_url(srv, _dbname(src, SRC_PREFIX))
    tgt_dbname = remote_url(srv, _dbname(tgt, TGT_PREFIX))
    doc = rdoc(src=src_dbname, tgt=tgt_dbname, filt=filt)
    if did in rdb:
        if reset:
            del rdb[did]
            rdb[did] = doc
            print "Created doc",doc['_id'],doc['_rev']
        else:
            old = rdb[did]
            print "Fetched old",old.id, old.rev
            old.update(doc)
            rdb[did] = old
            doc = old
    else:
        rdb[did] = doc
        print "Updated doc",doc['_id'], doc['_rev']
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
    rdb = getrdb(srv=srv, name=rdb)
    return clean_docs(prefix=PREFIX, db=rdb, srv=srv)

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
    db = getdb(dbname=_dbname(i, SRC_PREFIX), srv=srv, create=False, reset=False)
    return updocs(db=db, start=start, end=end, **kw)

def clean(rdb=None, srv=None):
    clean_replications(rdb=rdb, srv=srv)
    clean_target_dbs(srv=srv)
    clean_source_dbs(srv=srv)
    
def replicate_n_to_n(n=1, sleep_dt=0, reset=False, srv=None, filt=None):
    getrdb(srv)
    create_target_dbs(n, reset=reset, srv=srv)
    print n,"target dbs created"
    create_source_dbs(n, reset=reset, srv=srv, filt=filt)
    print n,"source dbs created"
    for i in range(1,n+1):
        try:
            rdoc = create_replication(src=i, tgt=i, reset=True, srv=srv, filt=filt)
        except Exception,e:
            print "Warning: raised exception",e
            time.sleep(1)
            print "continuing"
            continue
        time.sleep(sleep_dt)
        print i," : ",rdoc['_id'],rdoc['_rev']
        
def replicate_1_to_n(n=1, sleep_dt=0, reset=False, srv=None, filt=None):
    getrdb(srv)
    create_target_dbs(n, reset=reset, srv=srv)
    print n,"target dbs created"
    create_source_dbs(1, reset=reset, srv=srv, filt=filt)
    print "created source db"
    for i in range(1,n+1):
        try:
            rdoc = create_replication(src=1, tgt=i, reset=True, srv=srv, filt=filt)
        except Exception,e:
            print "Warning: raised exception",e
            time.sleep(1)
            print "continuing"
            continue
        time.sleep(sleep_dt)
        print i," : ",rdoc['_id'],rdoc['_rev']


def replicate_n_to_1(n=1, sleep_dt=0, reset=False, srv=None, filt=None):
    getrdb(srv)
    create_source_dbs(n, reset=reset, srv=srv, filt=filt)
    print n,"source dbs created"
    create_target_dbs(1, reset=reset, srv=srv)
    print "created target db"
    for i in range(1,n+1):
        try:
            rdoc = create_replication(src=i, tgt=1, reset=True, srv=srv, filt=filt)
        except Exception,e:
            print "Warning: raised exception",e
            time.sleep(1)
            print "continuing"
            continue
        time.sleep(sleep_dt)
        print i," : ",rdoc['_id'],rdoc['_rev']


def check_untriggered(rdb=None, srv=None):
    srv=getsrv(srv)
    rdb=getrdb(srv=srv, name=rdb, create=True)
    res = {}
    for did in rdb:
        doc = rdb[did]
        if 'source' in doc and 'target' in doc:
            _replication_state = doc.get('_replication_state')
            if _replication_state != "triggered":
                res[did] = _replication_state
    return res

def bump_replication_docs(n=0,rdb=None, srv=None):
    srv = getsrv(srv)
    rdb = getrdb(srv=srv, name=rdb, create=False)
    cnt = 0
    for did in rdb:
        doc = rdb[did]
        doc['junk'] = uuid.uuid4().hex
        rdb.save(doc)
        
        print "bumped",did,"@",doc['_rev']
        cnt+=1
        if n > 0 and cnt > n:
            print "bumped",n,"docs"

def tdb(i,srv=None):
    srv = getsrv(srv)
    return getdb(dbname=_dbname(i, TGT_PREFIX), srv=srv, create=False)


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
    
        
