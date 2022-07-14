"""
This module is responsible for configuration. It uses ConfigArgParse package
ConfigArgParse knows how to obtain configuration from various sources and it
uses this override order:

 code defaults > ~/.couchdyno.cfg > env vars REP_* > command line

Configuration is defined in the CFG_DEFAULTS module list.

The main function in this module is getcfg() it returns an opaque configuration
object which has configuration options as attributes.

Example of usage:

  opts = cfg.getcfg()
  timeout = opts.connection_timeout
  ...
"""

import re
import configargparse


# Specify list of files where to look for config options to load
CFG_FILES = ["~/.couchdyno.cfg", "./couchdyno.cfg", "~/.dyno.cfg", "./dyno.cfg"]


CFG_DEFAULTS = [
    #  --configname, default, env_varname, help_string
    ("prefix", "cdyno", "REP_PREFIX", "Prefix used for dbs and docs"),
    ("timeout", 0, "REP_TIMEOUT", "Client socket timeout"),
    (
        "cycle_timeout",
        8 * 3600,
        "REP_CYCLE_TIMEOUT",
        "How long to wait for changes to propagate",
    ),
    (
        "server_url",
        "http://adm:pass@localhost:15984",
        "REP_SERVER_URL",
        "Default server URL",
    ),
    (
        "source_url",
        None,
        "REP_SOURCE_URL",
        "Source URL. If not specified uses server_url",
    ),
    (
        "target_url",
        None,
        "REP_TARGET_URL",
        "Target URL. If not specified uses server_url",
    ),
    ("num_docs", 1, "NUM_DOCS", "Number of docs to insert on the target"),
    ("num_revs", 1, "NUM_REVS", "Number of revisions to insert for each doc"),
    (
        "num_branches",
        1,
        "NUM_BRANCHES",
        "Number of revision paths, each num_revs long to insert",
    ),
    (
        "reset_target",
        True,
        "RESET_TARGET",
        "Whether to reset target db on every cycle",
    ),
    (
        "reset_source",
        True,
        "RESET_SOURCE",
        "Whether to reset source db on every cycle",
    ),
    (
        "skip_rev_check",
        True,
        "SKIP_REV_CHECK",
        "Verify revisions for normal replications or just wait for job status to be completed",
    ),
    (
        "delete_before_updating",
        True,
        "DELETE_BEFORE_UPDATING",
        "Try to delete all the endopint docs before updating them",
    ),
    ("worker_processes", 4, "REP_WORKER_PROCESSES", "Replication parameter"),
    ("connection_timeout", 30000, "REP_CONNECTION_TIMEOUT", "Replicatoin paramater"),
    ("http_connections", 20, "REP_HTTP_CONNECTIONS", "Replication parameter"),
    ("create_target", False, "REP_CREATE_TARGET", "Replication parameter"),
    ("use_checkpoints", True, "REP_USE_CHECKPOINTS", "Replication parameter"),
    (
        "retries_per_request",
        10,
        "REP_RETRIES_PER_REQUEST",
        "Replication retries_per_request parameter",
    ),
    ("proxy", None, "REP_PROXY", "Replication proxy"),
    #  Settings below apply when using a locally running cluster
    #  This cluster can be controlled from the test framework, nodes can be
    #  stopped, its data directory can be modified, and so on.
    (
        "cluster_repo",
        None,
        "REP_CLUSTER_REPO",
        "File system path or Git URL of a CouchDB 2.x+ cluster",
    ),
    (
        "cluster_branch",
        None,
        "REP_CLUSTER_BRANCH",
        "If local cluster if fetched from Git, can specify a branch/commit",
    ),
    (
        "cluster_tmpdir",
        None,
        "REP_CLUSTER_TMPDIR",
        "If using a local cluster can provide a custom temp directory. This "
        " could be a RAM disk for example, or a directory provided by the test "
        " framework",
    ),
    (
        "cluster_reset_data",
        True,
        "REP_CLUSTER_RESET_DATA",
        "Reset data in the ./dev/lib/ before each cluster start?",
    ),
    (
        "cluster_settings",
        None,
        "REP_CLUSTER_SETTINGS",
        "Comma separated settings which look lik section.key=val",
    ),
]

_parser = None
_remaining_args = None
_opts = None


def logger(*args):
    if not args:
        print("")
    if isinstance(args[0], int) or isinstance(args[0], bool):
        if not args[0]:
            return
        args = args[1:]
    logstr = " ".join([str(a) for a in args])
    if not logstr:
        return
    if logstr[0] == "\n":
        print()
        logstr = logstr[1:]
    print(" > ", logstr)


def parse():
    global _parser, _opts, _rest_args
    if not _parser:
        _parser = _get_parser()
        _opts, _rest_args = _parser.parse_known_args()
    _validate_prefix(_opts.prefix)
    return _opts, _rest_args


def getcfg():
    """
    Return configuration object from configparse module.
    This object contains configuration gathered from multiple sources:
     * defaults in this module
     * config files
     * environment variables
     * command-line arguments
    """
    c = parse()[0]
    for (k, v) in vars(c).items():
        if v == 'true' or v == 'True':
            setattr(c, k, True)
        if v == 'false' or v == 'False':
            setattr(c, k, False)
    return c

def cfghelp():
    if _parser is None:
        parse()
    return _parser.format_help()


def unused_args():
    return parse()[1]


#  Private functions


def _get_parser():
    p = configargparse.ArgParser(default_config_files=CFG_FILES)
    for (name, dflt, ev, hs) in CFG_DEFAULTS:
        aname = "--" + name
        p.add_argument(aname, default=dflt, env_var=ev, help=hs)
    return p


def _validate_prefix(prefix):
    assert re.match(
        "^[a-z0-9]{3,50}", prefix, re.IGNORECASE
    ), "Prefix must be between 3 and 50 chars and have letters and numbers"
