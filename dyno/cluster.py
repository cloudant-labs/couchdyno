import os
import time
import shlex
import tempfile
import subprocess as sp
from ConfigParser import SafeConfigParser as Ini

USER='adm'
PASSWORD='pass'
KILL_TIMEOUT=5

def run(cmd, **kw):
    return sp.check_output(shlex.split(cmd), **kw)


class Cluster(object):

    def __init__(self, src, tmpdir=None, settings=None, user=USER, password=PASSWORD):
        src = os.path.realpath(os.path.abspath(src))
        self._validate_dir(src)
        dev_run = os.path.join(src, "dev", "run")
        assert os.path.exists(dev_run)
        assert os.path.exists(self._ini(src))
        self.run("which erl")
        self.run("which make")
        self.run("which rsync")
        self._maybe_configure_src(src)
        self.orig_tmpdir = tmpdir
        self.tmpdir = self._tmpdir(tmpdir)
        self.src = src
        self.settings = settings
        self.user = user
        self.password = password
        self.proc = None
        self.workdir = None
        self.url = None
        self.devnull = open(os.devnull, 'wb')

    @property
    def alive(self):
        if not self.proc:
            return False
        return self.proc.poll() is None

    def start(self):
        self.stop()
        workdir = self._cp(self.tmpdir)
        self._make(workdir)
        self._override_settings(workdir, self.settings)
        cmd = "dev/run --admin=%s:%s" % (self.user, self.password)
        print "Starting:", cmd, "in:", workdir
        self.proc = sp.Popen(cmd, cwd=workdir, stdout=self.devnull, shell=True)
        time.sleep(3)
        if not self.alive:
            sout, _ = self.proc.communicate()
            self.proc = None
            self.url = None
            #run("rm -rf %s" % workdir)
            raise Exception("Could not launch cluster: '%s'" % sout)
        self.workdir = workdir
        self.url = 'http://%s:%s@localhost:15984' % (self.user, self.password)
        return self.url

    __enter__ = start

    def stop(self):
        if not self.alive:
            return
        self.proc.terminate()
        tf = time.time() + KILL_TIMEOUT
        while time.time() < tf:
            if not self.alive:
                self.proc = None
                self.url = None
                return
            time.sleep(0.5)
        self.proc.kill()
        self.proc = None
        self.workdir = None
        self.url = None

    def __exit__(self, exc_type, exc_val, trace):
        self.stop()


    # Private methods

    def _tmpdir(self, tmpdir):
        if tmpdir is None:
            tmpdir = tempfile.mkdtemp(prefix="rdyno_cluster_tmpdir_")
            print "Created temp directory: ", tmpdir
        self._validate_dir(tmpdir)

    def _maybe_configure(self, src):
        src_dir = os.path.join(src, "src")
        if os.path.exists(src_dir):
            return
        print "Running './configure' in", src
        cloudant_config = os.path.join(src, "rebar.config.paas.script")
        asf_config = os.path.join(src, "rebar.config.script")
        if os.path.exists(cloudant_config):
            return run("./configure paas", cwd=src)
        elif os.path.exists(asf_config):
            return run("./configure --disable-fauxtong --disable-docs")
        else:
            raise Exception("Could not find rebar config file in %s" % src)

    def _ini(self, src):
        return os.path.join(src, "rel", "overlay", "etc", "default.ini")

    def _validate_dir(self, dpath):
        assert isinstance(dpath, basestring)
        assert dpath != '/'
        assert os.path.isabs(dpath)
        assert os.path.exists(dpath)
        assert os.path.isdir(dpath)

    def _cp(self, tmpdir):
        workdir = os.path.join(tmpdir, "db")
        run("mkdir -p %s" % workdir)
        #run("rm -rf  %s" % workdir)
        run("rsync --del -rlHDC %s/ %s" % (self.src, workdir))
        return workdir

    def _make(self, workdir):
        return run("make", cwd=workdir, shell=True)

    def _override_settings(self, workdir, settings):
        if not settings:
            return
        assert isinstance(settings, list)
        assert isinstance(settings[0], tuple)
        assert len(settings[0]) == 3
        ini = Ini()
        fp = self._ini(workdir)
        ini.read(fp)
        for section, key, val in settings:
            if not ini.has_section(section):
                ini.add_section(section)
            print "Setting",fp,section,key,"=",val
            ini.set(section, key, val)
        with open(fp, 'w') as fh:
            ini.write(fh)
