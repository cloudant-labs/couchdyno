"""
This module is responsible for configuration. It uses ConfigArgParse package
ConfigArgParse knows how to obtain configuration from various sources and it
and used a fixed override order:

 code defaults > ~/.dyno.cfg config file > env vars REP_* > command line

Configuration are defined in the CFG_DEFAULTS module list.

The main function in this module is getcfg() it returns an opaque configuration
object which has configuration options as attributes.

Example of usage:

  opts = cfg.getcfg()
  timeout = opts.connection_timeout
  ...
"""

import configargparse


# Specify list of files where to look for config options to load
CFG_FILES = ['~/.dyno.cfg']


CFG_DEFAULTS = [

    #  --configname, default, env_varname, helpstring

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

    ('local_cluster', None, 'REP_LOCAL_CLUSTER',
     'File system path or Git URL of a CouchDB 2.x+ cluster'),

    ('local_cluster_branch', None, 'REP_LOCAL_CLUSTER_BRANCH',
     'If local cluster if fetched from Git, can specify a branch/commit'),

    ('local_cluster_tmpdir', None, 'REP_LOCAL_CLUSTER_TMPDIR',
     'If using a local cluster can provide a custom temp directory. This could'\
     ' be a RAM disk for example, or a directory provided by the test framework')
]



_parser = None

def getcfg():
    """
    Return configuration object from configparse module.
    This object contains configuration gathered from multiple sources:
     * defaults in this module
     * config files
     * environment variables
     * command-line arguments
    """
    global _parser
    if _parser is None:
        _parser = _get_parser()
    return _parser.parse_known_args()[0]

def cfghelp():
    global _parser
    if _parser is None:
        _parser = _get_parser()
    return _parser.format_help()


#  Private functions

def _get_parser():
    p = configargparse.ArgParser(default_config_files=CFG_FILES)
    for (name, dflt, ev, hs) in CFG_DEFAULTS:
        aname = '--' + name
        if dflt is False:
            p.add_argument(aname, default=dflt, action="store_true", env_var=ev, help=hs)
        else:
            p.add_argument(aname, default=dflt, env_var=ev, help=hs)
    return p
