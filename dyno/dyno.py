import time
import random
import string
import argparse
import datetime
import urlparse
import couchdb  # pip : CouchDB==1.0
from couchdb.design import ViewDefinition

DEFAULT_TOTAL = 1000
DEFAULT_SIZE = 1000
DEFAULT_UPDATES = 10
VERSION = 1
IDPAT = 'dyno_%012d'
HISTORY_MAX = 1000
FILL_BATCH = 100000

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
    p.add_argument('-w', '--wait-to-fill', action="store_true", default=False,
                   help='Fill database until total number of docs')
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
    if args.wait_to_fill:
        print
        print "Filling up database..."
        print
        left = args.total
        while left > FILL_BATCH:
            left -= FILL_BATCH
            _update_docs(db, metadoc, updates=FILL_BATCH)
            print
        _update_docs(db, metadoc, updates=left)
        print "Database filled"
    print "Run 'dyno-execute' periodically to start updating documents."
    exit(0)


def execute():
    """
    Script endpoint for execute. This should be called peridically.
    """
    p = _argparser('Execute load test cycle. Can override update-per-run')
    p.add_argument('-c', '--continuous', type=int, default=0,
                   help='Run continuously and sleep these many'
                   'seconds between runs'),
    p.add_argument('-u', '--updates-per-run', type=int,
                   default=0,
                   help='Overrides # of updates for this particular execution')
    args = p.parse_args()
    db = _get_db(args.dburl, create=False)
    if db is None:
        print "ERROR: DB not found. Did you run dyno-setup first?"
        exit(3)
    metadoc = MetaDoc().load(db)
    c = 0
    while True:
        new_metadoc = _update_docs(db, metadoc, args.updates_per_run)
        if args.continuous <= 0:
            break
        c += 1
        print "Sleeping", args.continuous, "seconds before next run", c
        time.sleep(args.continuous)
        print
    print "New state:"
    new_metadoc.pprint()
    exit(0)


def info():
    """
    Script endpoint to show setup and run statistics for dyno.
    """
    p = _argparser('Get load test information')
    p.add_argument('-d', '--daily-census', action="store_true", default=False,
                   help='Print a daily census (which days had updates)')
    p.add_argument('-c', '--conflicts', action="store_true", default=False,
                   help='Print a conflicts report')
    args = p.parse_args()
    db = _get_db(args.dburl, create=False)
    if db is None:
        print "ERROR: DB not found. Did you run dyno-setup first?"
        exit(1)
    info = db.info()
    print "DB Info:"
    print " . disk_size:", info['disk_size']
    print " . doc_count:", info['doc_count']
    print " . update_seq:", info['update_seq'][:40]+'...'

    metadoc = MetaDoc().load(db)
    print "Dyno Info:"
    metadoc.pprint()
    history = metadoc['history']
    print "Update History:"
    print " . updates", len(history), "/ max kept", HISTORY_MAX
    if len(history) > 1:
        t0, tl = history[0][0], history[-1][0]
        t = int(tl - t0)
        t_hours = int(t/3600)
        t_days = '%0.1f' % (t_hours/24.0)
        print " . earliest update (utc):", _ts_to_iso(t0)
        print " . last update (utc):", _ts_to_iso(tl)
        print " . interval (sec/hours/days):", t, "/", t_hours, "/", t_days
        max_errors = 0
        rates = []
        for _, dt, _, updates, errors in history:
            if dt > 0:
                rates.append(updates/float(dt))
            max_errors = max(max_errors, errors)
        if max_errors:
            print " . max errors seen:", max_errors
        if len(rates) > 0:
            avg_rate = int(sum(rates) / len(rates))
            print " . avg doc update rate:", avg_rate, "/ sec"
    if args.conflicts:
        _info_conflicts(db)
    if args.daily_census:
        _info_days(db)
    exit(0)


# Private helper functions

def _wait_for_view(db, view, maxwait=100000):
    till = time.time() + maxwait
    while True:
        try:
            for r in view(db, descending=True, limit=1):
                pass
            return
        except couchdb.http.ServerError, ex:
            ex_args = ex.args[0]
            if (
                    isinstance(ex_args, tuple) and
                    len(ex_args) == 2 and
                    ex_args[0] == 500 and
                    isinstance(ex_args[1], tuple) and
                    ex_args[1][0] == 'timeout'
            ):
                time_left = int(till - time.time())
                if time_left <= 0:
                    print "ERROR: view", view.name, "took >", maxwait
                    raise
                print "... waiting for view, time left:", time_left, "sec."
                continue
            else:
                raise


def _info_conflicts(db):
    view = _conflicts_view()
    view.sync(db)
    _wait_for_view(db, view)
    vres = list(view(db))
    if len(vres) == 1:
        print "Conflicts:", vres[0].value
    elif len(vres) > 1:
        raise ValueError("Expected 1 result from view. Got: " + str(len(vres)))


def _info_days(db):
    view = _times_view()
    view.sync(db)
    _wait_for_view(db, view)
    days = set()
    for r in db.iterview(view.design+'/'+view.name, batch=100000):
        dinst = datetime.datetime.utcfromtimestamp(int(r.key))
        days.add(dinst.strftime('%Y-%m-%d'))
    print "Doc update census (per day):"
    for d in sorted(days):
        print " ->", d


def _times_view():
    return ViewDefinition('dyno_times', 'times', '''
function(doc) {
      if(doc._id.indexOf("dyno_") === 0 &&  doc.ts) {
         emit(doc.ts, null);
      }
}
''')


def _conflicts_view():
    return ViewDefinition('dyno_conflicts', 'conflicts', '''
function(doc) {
      if(doc._id.indexOf("dyno_") === 0 && doc._conflicts) {
         emit(doc._id, doc._conflicts.length);
      }
}
''', reduce_fun='_sum')


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
                   created=int(time.time()),
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
            raise Exception("%s missing in %s, was run dyno-setup run?" % (
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
            if k == '_id' or k == '_rev' or k == 'history':
                continue
            elif k == 'last_ts':
                v = "%s (%s)" % (v, _ts_to_iso(v))
            elif k == 'created' and isinstance(v, int):
                v = "%s (%s)" % (v, _ts_to_iso(v))
            print " .", str(k), ":", str(v)


def _get_db(dburl, create=True, reset_db=False):
    """
    Get a db handle. Optionally reset / create.
    """
    sres = urlparse.urlsplit(dburl)
    dbname = sres.path.lstrip('/')
    srv = couchdb.Server(urlparse.urlunsplit(sres._replace(path='', query='')))
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
    table = [(2e3, 2000), (1e4, 1000), (1e5, 200), (2e6, 10)]
    for (limit, bsize) in table:
        if docsize < limit:
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
                             batch=2000):
            revs[str(r.id)] = str(r.value['rev'])
    return revs


def _random_data(size):
    return ''.join(random.choice(string.ascii_lowercase) for _ in xrange(size))


def _bulk_update(db, docrevs, size, ts, docids):
    """
    Update one batch using bulk docs updates. Return
    number of successfully updated docs.
    """
    docs = []
    for _id in docids:
        doc = dict(_id=_id, ts=ts, data=_random_data(size))
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


def _update_docs(db, metadoc, updates=0):
    """
    Main logic. Start at metadoc['start'] and
    update next metadoc['updates'] documents (can be
    overriden by optional updates parameters). If needed
    wrap around back to start. When done, checkpoint
    meta document to db.
    """
    t0 = time.time()
    total = metadoc['total']
    size = metadoc['size']
    updates = updates if updates else metadoc['updates']
    start = metadoc['start']
    batchsize = _batch_size(size)
    int1, int2 = _intervals(start, updates, total)
    docint1, docint2 = [IDPAT % i for i in int1], [IDPAT % i for i in int2]
    print "Total:", total, " size:", size, " @:", start, " updating:", updates
    trev0 = time.time()
    docrevs = _docrevs(db, docint1, docint2)
    trevdt = time.time() - trev0
    revcount = len(docrevs)
    trevrate = int(revcount / trevdt)
    if docrevs:
        print "%s revs, %.1f sec, @ %s revs/sec" % (revcount, trevdt, trevrate)
    docint1.extend(docint2)
    ok = 0
    for i in range(0, len(docint1), batchsize):
        docid_batch = docint1[i:i+batchsize]
        ok += _bulk_update(db, docrevs, size, int(t0), docid_batch)
    errors = updates - ok
    dt = time.time() - t0
    rate = int(updates / dt)
    print "Updated %s dt: %.3f sec @ %s docs/sec" % (updates, dt, rate)
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
