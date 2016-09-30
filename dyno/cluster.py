import os
import time
import copy
import shlex
import socket
import atexit
import tempfile
import subprocess as sp
from ConfigParser import SafeConfigParser as Ini
from cfg import getcfg
from rep import Rep


USER = 'adm'
PASSWORD = 'pass'
KILL_TIMEOUT = 5
N1_PORT = 15984
LOG_COMMANDS = True


def get_cluster(cfg=None):
    if cfg is None:
        cfg = getcfg()
    if cfg.cluster_repo is None:
        return None
    return Cluster(cfg=cfg)


class Cluster(object):

    #  _registered and _running keep track of running
    #  cluster instances in order to do cleanup on process exit
    _registered = False
    _running = set()

    def __init__(self, cfg=None):
        """
        Instantiate a Cluster configuration. This doesn't run the cluster just
        configures it. This involves making sure ./congure has run,
        and some command line tools and dependencies are present.
        """
        self.proc = None
        self.workdir = None
        self.url = None
        self.src = None
        self.rep = None
        self.user = USER
        self.password = PASSWORD
        self.port = N1_PORT
        if cfg is None:
            cfg = getcfg()
        if not cfg.cluster_repo:
            raise ValueError("Cluster needs a 'cluster_repo' setting %s" % cfg)
        repo = cfg.cluster_repo
        if cfg.cluster_branch:
            src = (repo, cfg.cluster_branch)
        else:
            src = repo
        self.orig_tmpdir = cfg.cluster_tmpdir
        self.tmpdir = self._tmpdir(cfg.cluster_tmpdir)
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
        self.settings = _parse_settings(cfg.cluster_settings)
        self.reset_data = cfg.cluster_reset_data
        self.cfg = cfg
        self._maybe_register_atexit()

    def get_rep(self):
        if not self.alive:
            raise Exception("Cluster %s not running" % self)
        cfg = copy.deepcopy(self.cfg)
        cfg.server_url = self.url
        return Rep(cfg=cfg)

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
        time.sleep(4)
        if not self.alive:
            sout, _ = self.proc.communicate()
            self.proc = None
            self.url = None
            # run("rm -rf %s" % workdir)
            raise Exception("Could not launch cluster: '%s'" % sout)
        self._running.add(self)
        self.workdir = workdir
        self.url = 'http://%s:%s@localhost:%s' % (
            self.user, self.password, self.port)
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

    def stop_node(self, node_id):
        if not isinstance(node_id, int):
            raise ValueError("Node ID should be %s an integer" % node_id)
        cmd = "pkill -f 'beam.smp.*node%s@127.0.0.1 -setcookie monster'"
        return run(cmd % node_id, stdout=self.devnull, skip_check=True)

    # Private methods

    def __eq__(self, o):
        if not isinstance(o, Cluster):
            return False
        return (self.src, self.port) == (o.src, o.port)

    def __hash__(self):
        return hash((self.src, self.port))

    def __str__(self):
        if self.alive:
            running_str = "y"
        else:
            running_str = "n"
        return "<Cluster %s workdir:%s port:%s running?:%s>" % (
            self.src, self.workdir, self.port, running_str)
    __repr__ = __str__

    def _maybe_register_atexit(self):
        if not self.__class__._registered:
            atexit.register(self.__class__._atexit_cleanup)
            self.__class__._registered = True

    def _checkport(self, port, tries=10):
        while tries > 0:
            try:
                s = socket.socket()
                s.bind(('127.0.0.1', port))
                s.close()
                return
            except socket.error:
                if tries > 0:
                    time.sleep(5)
                    tries -= 1
                    print "Port 15984 is in use, waiting. Tries left:", tries
                    continue
                else:
                    raise Exception("Port %s is already in use " % port)

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
        print "Trying to run make."
        run("make", cwd=src, stdout=self.devnull)

    def _ini(self, src):
        return os.path.join(src, "rel", "overlay", "etc", "default.ini")

    def _validate_dir(self, dpath):
        assert dpath, "Cannot have an empty directory path"
        assert isinstance(dpath, basestring), "Directory path must be a string"
        dpath = os.path.abspath(os.path.expanduser(dpath))
        dpath = os.path.realpath(dpath)
        assert dpath != '/', "Cannot use root, probably a mistake"
        assert os.path.isdir(dpath), "Directory doesn't exist"
        return dpath

    def _cp(self, tmpdir):
        workdir = os.path.join(tmpdir, "db")
        run("mkdir -p %s" % workdir)
        run("rsync --del -rlHDC %s/ %s" % (self.src, workdir))
        return workdir

    def _override_settings(self, workdir, extra):
        extra = _parse_settings(extra)
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
            ini.set(section, key, str(val))
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

    @classmethod
    def _atexit_cleanup(cls):
        for cluster in list(cls._running):
            print "\nStopping cluster:", cluster
            cluster.stop()
            print "\nCleaning up cluster:", cluster
            cluster.cleanup()


def run(cmd, **kw):
    skip_check = kw.pop('skip_check', False)
    if LOG_COMMANDS:
        print "  RUN %s" % cmd
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


def _parse_settings(settings):
    if not settings:
        return None
    if isinstance(settings, list):
        return [_parse_setting(s) for s in settings]
    elif isinstance(settings, basestring):
        settings = settings.strip()
        return [_parse_setting(s) for s in settings.split(',')]
    raise ValueError("Invalid settings specification: %s" % settings)


def _parse_setting(setting):
    if isinstance(setting, tuple) and len(setting) == 3:
        return setting
    elif isinstance(setting, basestring):
        section, val_and_eq = setting.split('.', 1)
        val, eq = val_and_eq.rsplit('=', 1)
        return (section, val, eq)
    raise ValueError("Invalid setting %s" % setting)
