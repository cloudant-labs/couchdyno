import os
import time
import shlex
import socket
import atexit
import tempfile
import subprocess as sp
from ConfigParser import SafeConfigParser as Ini

USER = 'adm'
PASSWORD = 'pass'
KILL_TIMEOUT = 5
N1_PORT = 15984
LOG_COMMANDS = True


class Cluster(object):

    #  _registered and _running keep track of running
    #  cluster instances in order to do cleanup on process exit
    _registered = False
    _running = set()

    def __init__(self,
                 src,  # path to a dbnext / asf couchdb repo
                 tmpdir=None,  # optional temp dir, maybe on a ramdisk
                 settings=None,  # [(section, key, val),...] settings
                 reset_data=True):
        """
        Instantiate a Cluster configuration. This doesn't run the cluster just
        configures it. This involves making sure ./congure has run,
        and some command line tools and dependencies are present.
        """
        self.orig_tmpdir = tmpdir
        self.tmpdir = self._tmpdir(tmpdir)
        src = self._get_source(src)
        dev_run = os.path.join(src, "dev", "run")
        assert os.path.exists(dev_run)
        assert os.path.exists(self._ini(src))
        # Validate some tools used later
        self.devnull = open(os.devnull, 'wb')
        run("which erl", stdout=self.devnull)
        run("which erlc", stdout=self.devnull)
        run("which make", stdout=self.devnull)
        run("which rsync", stdout=self.devnull)
        run("which pkill", stdout=self.devnull)
        self._maybe_configure(src)
        self.src = src
        self.settings = settings
        self.user = USER
        self.password = PASSWORD
        self.proc = None
        self.workdir = None
        self.url = None
        self.port = N1_PORT
        self.reset_data = reset_data
        self._maybe_register_atexit()

    def cleanup(self):
        if self.orig_tmpdir:
            return
        run("rm -rf %s" % self.tmpdir)

    @property
    def alive(self):
        if not self.proc:
            return False
        return self.proc.poll() is None

    def running(self, settings=None):
        """
        This returns a context manager instance. So can do stuff like:
          c = Cluster(...)
          with c.running([('replicator', 'max_jobs', '15')]) as crun:
              db = couchdb.Server(crun.url)
              crun.stop_node(1)
        """
        return _Ctx(self, settings=settings)

    def start(self, settings=None):
        """
        Start the cluster in a tmp work directory:
         - Copy any changes from original repo
         - Maybe run make (this can be used when developing)
         - Check of port of node1 is available
         - Start a ./dev/run subprocess
         - Verify still up after a short period of time
         - Assume cluster started fine, save subprocess Popen object and return
        """
        self.stop()
        workdir = self._cp(self.tmpdir)
        print "Working directory:", workdir
        print "Running make."
        run("make", cwd=workdir, stdout=self.devnull)
        if self.reset_data:
            data_dir = os.path.join(workdir, "dev", "lib")
            print "Resetting data dir: ", data_dir
            run("rm -rf %s/*" % data_dir)
            log_dir = os.path.join(workdir, "dev", "logs")
            print "Cleaning logs:", log_dir
            run("rm -rf %s/*" % log_dir)
        self._checkport(self.port)
        self._override_settings(workdir, settings)
        cmd = "dev/run --admin=%s:%s" % (self.user, self.password)
        print "Starting:", cmd
        self.proc = sp.Popen(cmd, cwd=workdir, stdout=self.devnull, shell=True)
        time.sleep(3)
        if not self.alive:
            sout, _ = self.proc.communicate()
            self.proc = None
            self.url = None
            #run("rm -rf %s" % workdir)
            raise Exception("Could not launch cluster: '%s'" % sout)
        self._running.add(self)
        self.workdir = workdir
        self.url = 'http://%s:%s@localhost:%s' % (self.user, self.password, self.port)
        return self

    def stop(self):
        if not self.alive:
            print "Trying to stop other dev cluster instances..."
            kill_nodes = "pkill -f 'beam.smp.*127.0.0.1 -setcookie monster'"
            run(kill_nodes, stdout=self.devnull, skip_check=True)
            kill_dev_run = "pkill -f 'dev/run --admin='"
            run(kill_dev_run, stdout=self.devnull, skip_check=True)
            self._running.discard(self)
            return
        self.proc.terminate()
        tf = time.time() + KILL_TIMEOUT
        while time.time() < tf:
            if not self.alive:
                self._running.discard(self)
                self.proc = None
                self.url = None
                return
            time.sleep(0.5)
        self.proc.kill()
        self._running.discard(self)
        self.proc = None
        self.workdir = None
        self.url = None
        return self

    def stop_node(self,node_id):
        if not isinstance(node_id, int):
            raise ValueError("Node ID should be %s an integer" % node_id)
        kill_cmd = "pkill -f 'beam.smp.*node%s@127.0.0.1 -setcookie monster'" % node_id
        return run(kill_cmd, stdout=self.devnull, skip_check=True)

    # Private methods

    def __eq__(self, o):
        if not isinstance(o, Cluster):
            return False
        return (self.src, self.port) == (o.src, o.port)

    def __hash__(self):
        return hash((self.src, self.port))

    def __str__(self):
        return "Cluster src:%s workdir:%s port1:%s" % (self.src, self.workdir, self.port)
    __repr__ = __str__

    @classmethod
    def _atexit_cleanup(cls):
        print "Running atexit cleanup hook."
        for cluster in list(cls._running):
            print "Stopping cluster:", cluster
            cluster.stop()

    def _maybe_register_atexit(self):
        if not self.__class__._registered:
            atexit.register(self.__class__._atexit_cleanup)
            print "Registered atexit cleanup hook"
            self.__class__._registered = True

    def _checkport(self, port, tries=10):
        while tries > 0:
            s = socket.socket()
            try:
                s.bind(('127.0.0.1', port))
                s.close()
                return
            except socket.error,e:
                if tries > 0:
                    time.sleep(5)
                    tries -= 1
                    print "Port 15984 is in use, waiting. Tries left:", tries
                    continue
                else:
                    raise Exception("Looks like port %s is already in use " % port)

    def _tmpdir(self, tmpdir):
        if tmpdir is None:
            tmpdir = tempfile.mkdtemp(prefix="rdyno_cluster_tmpdir_")
            print "Created temp directory: ", tmpdir
        tmpdir = os.path.realpath(os.path.abspath(os.path.expanduser(tmpdir)))
        print "Using tmpdir:", tmpdir
        return self._validate_dir(tmpdir)

    def _maybe_configure(self, src):
        src_dir = os.path.join(src, "src")
        if not os.path.exists(src_dir):
            print "Running './configure' in", src
            cloudant_config = os.path.join(src, "rebar.config.paas.script")
            asf_config = os.path.join(src, "rebar.config.script")
            if os.path.exists(cloudant_config):
                run("./configure paas", stdout=self.devnull, cwd=src)
            elif os.path.exists(asf_config):
                run("./configure --disable-fauxton --disable-docs",
                    stdout=self.devnull, cwd=src)
            else:
                raise Exception("Could not find rebar config file in %s" % src)

    def _ini(self, src):
        return os.path.join(src, "rel", "overlay", "etc", "default.ini")

    def _validate_dir(self, dpath):
        assert dpath, "Cannot have an empty directory path"
        assert isinstance(dpath, basestring), "Directory path must be a string"
        dpath = os.path.abspath(os.path.expanduser(dpath))
        dpath = os.path.realpath(dpath)
        assert dpath != '/', "Cannot use top level directory, probably a mistake"
        assert os.path.isdir(dpath)
        return dpath

    def _cp(self, tmpdir):
        workdir = os.path.join(tmpdir, "db")
        run("mkdir -p %s" % workdir)
        run("rsync --del -rlHDC %s/ %s" % (self.src, workdir))
        return workdir

    def _override_settings(self, workdir, extra):
        sset = set()
        if extra:
            sset.update(extra)
        if self.settings:
            sset.update(self.settings)
        settings = sorted(sset)
        if not settings:
            return
        for s in settings:
            print "  %s.%s = %s " % (s[0], s[1], s[2])
        assert isinstance(settings, list)
        assert isinstance(settings[0], tuple)
        assert len(settings[0]) == 3
        ini = Ini()
        fp = self._ini(workdir)
        ini.read(fp)
        for section, key, val in settings:
            if not ini.has_section(section):
                ini.add_section(section)
            ini.set(section, key, val)
        with open(fp, 'w') as fh:
            ini.write(fh)

    def _get_source(self, src):
        if isinstance(src, basestring) and '://' not in src:
            return self._validate_dir(src)
        dest = os.path.join(self.tmpdir, "src_clone")
        run("rm -rf %s" % dest)
        cmd_prefix = "git clone --depth 1 --single-branch "
        if isinstance(src, tuple) and len(tuple) == 2:
            cmd = cmd_prefix + " --branch %s %s %s" % (src[1], src[0], dest)
        else:
            cmd = cmd_prefix + "%s %s" % (src, dest)
        run(cmd, cwd=self.tmpdir)
        return dest

def run(cmd, **kw):
    skip_check = kw.pop('skip_check', False)
    if LOG_COMMANDS:
        print " * running:",cmd
    if skip_check:
        return sp.call(shlex.split(cmd), **kw)
    else:
        return sp.check_call(shlex.split(cmd), **kw)



class _Ctx(object):
    def __init__(self, cluster, settings=None):
        self.cluster = cluster
        self.settings = settings

    def __enter__(self):
        self.cluster.start(self.settings)
        return self.cluster

    def __exit__(self, exc_type, exc_val, trace):
        self.cluster.stop()
