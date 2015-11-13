import time
import os
import argparse
import datetime
import couchdb  # pip : CouchDB==1.0

DEFAULT_TOTAL = 1000
DEFAULT_SIZE = 1000
DEFAULT_UPDATES = 10
VERSION = 1
PATTERN = 'x'
IDPAT = 'dyno_%012d'
HISTORY_MAX = 100


# Command Line Entry Points


def setup():
    """
    Script endpoint for setup. This is for running once and setting up
    a database to be used for dyno-execute endpoint later.
    """
    p = _argparser('Setup load test. Creates DB and saves settings.')
    p.add_argument('-t', '--total', type=int, default=DEFAULT_TOTAL,
                   help='Number of docs to use')
    p.add_argument('-s', '--size', type=int, default=DEFAULT_SIZE,
                   help='Size of document')
    p.add_argument('-u', '--updates-per-run', type=int,
                   default=DEFAULT_UPDATES,
                   help='Update these many docs on each run')
    p.add_argument('-f', '--force', action="store_true", default=False,
                   help='Overwrite/reset existing db')
    args = p.parse_args()
    if not args.force:
        db = _get_db(args.dburl, create=False)
        if db is not None:
            print "ERROR: db:", args.dburl, "already exists"
            print " To force reset it, use -f|--force"
            exit(1)
        db = _get_db(args.dburl, create=True)
    else:
        db = _get_db(args.dburl, create=True, reset_db=True)
    metadoc = MetaDoc.from_args(args).save(db)
    print "Saved configuration:"
    metadoc.pprint()
    print "run dyno-execute to start updating documents."
    exit(0)


def execute():
    """
    Script endpoint for execute. This should be called peridically.
    """
    p = _argparser('Execute load test cycle. Can override update-per-run')
    p.add_argument('-c', '--continuous', type=int, default=0,
                   help='Run continuously and sleep these many'
                   'seconds between runs'),
    args = p.parse_args()
    db = _get_db(args.dburl, create=False)
    if db is None:
        print "ERROR: DB not found. Did you run dyno-setup first?"
        exit(3)
    metadoc = MetaDoc().load(db)
    c = 0
    while True:
        new_metadoc = _update_docs(db, metadoc)
        if args.continuous <= 0:
            break
        c += 1
        print "Sleeping", args.continuous, "seconds before next run", c
        time.sleep(args.continuous)
    print "New state:"
    new_metadoc.pprint()
    exit(0)


def info():
    """
    Script endpoint to show setup and run statistics for dyno.
    """
    p = _argparser('Get load test information')
    args = p.parse_args()
    db = _get_db(args.dburl, create=False)
    if db is None:
        print "ERROR: DB not found. Did you run dyno-setup first?"
        exit(1)
    metadoc = MetaDoc().load(db)
    metadoc.pprint()
    exit(0)


# Private helper functions

class MetaDoc(dict):
    """
    Simple class to represent and manipulate dyno metadata doc.
    """
    ID = 'dyno_meta'

    def __init__(self, *args, **kwargs):
        super(MetaDoc, self).__init__(*args, **kwargs)
        self['_id'] = self.ID

    @classmethod
    def from_args(cls, args):
        """
        Build metadoc from argparser args.
        """
        return cls(total=args.total,
                   size=args.size,
                   updates=min(args.updates_per_run, args.total),
                   created=time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                   version=VERSION,
                   start=0,
                   last_updates=0,
                   last_ts=0,
                   last_dt=0,
                   last_errors=0,
                   history=[],  # [[ts,dt,start,updates,errors],...]
                   )

    def load(self, db):
        """
        Load from the database.
        """
        metadoc = db.get(self.ID)
        if not metadoc:
            raise Exception("%s missing in %s, was run dyno-setup run?"(
                self.ID, db))
        self.update(metadoc)
        return self

    def checkpoint(self, db, start, ts, dt, updates, errors):
        """
        Update current metadata and save to db.
        """
        hline = [ts, dt, self['start'], updates, errors]
        self['start'] = start
        self['last_ts'] = ts
        self['last_dt'] = dt
        self['last_updates'] = updates
        self['last_errors'] = errors
        self['history'] = self['history'][-HISTORY_MAX:]+[hline]
        return self.save(db)

    def save(self, db):
        db.save(self)
        return self

    def pprint(self):
        for k, v in sorted(self.iteritems()):
            if k == '_id' or k == '_rev':
                continue
            if k == 'history':
                v = "(%s items)" % len(v)
            elif k == 'last_ts':
                v = "%s (%s)" % (v, _ts_to_iso(v))
            print " .", str(k), ":", str(v)


def _get_db(dburl, create=True, reset_db=False):
    """
    Get a db handle. Optionally reset / create.
    """
    dburl = dburl.rstrip('/')
    surl, dbname = os.path.split(dburl)
    srv = couchdb.Server(surl)
    srv.version()  # throws if can't connect to server
    if reset_db:
        if dbname in srv:
            del srv[dbname]
        return srv.create(dbname)
    if dbname in srv:
        return srv[dbname]
    else:
        if create:
            return srv.create(dbname)
        else:
            return None


def _batch_size(docsize):
    """
    Calculate bulk update batch size based on individual
    docsize. A simple guess-based interval
    table for now.
    """
    table = [(0,   2e3,  2000),
             (2e3, 1e4,  1000),
             (1e4, 1e5,  200),
             (1e5, 2e6,  10)]
    for (lo, hi, bsize) in table:
        if lo <= docsize < hi:
            return bsize
    return 1


def _intervals(start, updates, total):
    """
    Calculate 2 intervals based on wrap-around.
    So start+updates could point past end of docs,
    in that case go back to start.
    """
    updates = min(updates, total)
    wrap = max(0, start + updates - total)
    if wrap > 0:
        return xrange(start, total), xrange(0, wrap)
    else:
        return xrange(start, start + updates), xrange(0, 0)


def _docrevs(db, *intervals):
    """
    Given a db and *args of intervals,
    where an interval is a sorted list
    of doc ids, fetches revisions for those docs
    using an efficient range query (_all_docs).
    """
    revs = {}
    for interval in intervals:
        if not interval:
            continue
        for r in db.iterview('_all_docs',
                             startkey=interval[0],
                             endkey=interval[-1],
                             inclusive_end=True,
                             batch=1000):
            revs[str(r.id)] = str(r.value['rev'])
    return revs


def _bulk_update(db, docrevs, data, ts, docids):
    """
    Update one batch using bulk docs updates. Return
    """
    docs = []
    for _id in docids:
        doc = dict(_id=_id, ts=ts, data=data)
        _rev = docrevs.get(_id)
        if _rev is not None:
            doc['_rev'] = _rev
        docs.append(doc)
    res = db.update(docs)
    ok = 0
    for resdoc in res:
        if resdoc[0]:
            ok += 1
    return ok


def _ts_to_iso(ts):
    return datetime.datetime.utcfromtimestamp(ts).isoformat()


def _update_docs(db, metadoc):
    """
    Main logic. Start at metadoc['start'] and
    update next metadoc['updates'] documents. If needed
    wrap around back to start. When done checkpoint
    meta document to db.
    """
    t0 = time.time()
    total = metadoc['total']
    size = metadoc['size']
    updates = metadoc['updates']
    start = metadoc['start']
    batchsize = _batch_size(size)
    data = PATTERN * size
    int1, int2 = _intervals(start, updates, total)
    docint1, docint2 = [IDPAT % i for i in int1], [IDPAT % i for i in int2]
    print "Updating docs. Configuration:"
    metadoc.pprint()
    trev0 = time.time()
    docrevs = _docrevs(db, docint1, docint2)
    trevdt = time.time() - trev0
    trevrate = int(len(docrevs) / trevdt)
    print "%s revs, %.3f sec, @ %s revs/sec" % (len(docrevs), trevdt, trevrate)
    docint1.extend(docint2)
    ok = 0
    for i in range(0, len(docint1), batchsize):
        docid_batch = docint1[i:i+batchsize]
        ok += _bulk_update(db, docrevs, data, int(t0), docid_batch)
    errors = updates - ok
    dt = time.time() - t0
    rate = int(total / dt)
    print "updated %s docs: dt: %.3f sec, @ %s docs/sec" % (updates, dt, rate)
    if errors > 0:
        print "(!)errors:", errors
    return metadoc.checkpoint(db,
                              start=(start + updates) % total,
                              ts=int(t0),
                              dt=int(dt),
                              updates=updates,
                              errors=errors)


def _argparser(desc):
    """
    Common argparser code that seems to be needed by every
    script endpoint.
    """
    p = argparse.ArgumentParser(description=desc)
    p.add_argument('dburl', help='Full DB URL (can include user & pass)')
    return p
