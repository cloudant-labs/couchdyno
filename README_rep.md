
Rep
===

Rep started as interactive IPython-based toolbench used to
test and and experiment with CouchDB replications. It is designed to
be used interactively, via a command prompt. History is preserved across
runs so it is easy to re-use older snippets or scenarios.


Functionality
-------------

Can set up different replication patterns:

 * 1 source to N targets
 * N targets to 1 source
 * N sources to N targets
 * A chain of length N, for ex. 1->2->...->N
 * A connected cluster of N^2 replications

Can also configure:

 * How many documents to use for each database.
 * Make replications continuous or one-shot (normal)
 * Configurable number of attachments
 * Filters: Javascript, doc_ids, view, Mango
 * Can add arbitrary data to either replication docs or source docs
 * Separate replication, source and target clusters

It can even do odd things like put each replication document into a
a separate replication database (this could be used to test how multiple
replication databases are handled).



How To Get Started
------------------

By default `rep` utility assumes there is a local (dev) 3 node cluster.
With a user/password of `adm:pass`, first node listening on port 15984.

That is usually done like this:

```
$ cd <db_source_dir>
$ ./configure ...
$ make
$ ./dev/run --admin=adm:pass
```

In top level couchdyno repo run `./build.sh` on first use. It sets up a Python
venv and fetches dependencies.

Then lauch with `./venv/bin/rep`:

```
$ ./venv/bin/rep
Interactive replication toolbox
 rep, rep.getsrv, rep.getrdb and couchdb modules are auto-imported
 Assumes cluster runs on http://adm:pass@localhost:5984
 Type rep. and press <TAB> to auto-complete available functions

 Examples:

  * rep.rep.replicate_1_to_n_and_compare(2, cycles=2)
    # replicate 1 source to 2 targets (1->2, 1->3). Fill source with data
    # (add a document) and then wait for all targets to have same data.
    # Do it 2 times (cycles=2).

  * rep.getsrv() # get a CouchDB Server instance
In [1]:
```

How To Use
-----------

A lot of the module level functions can be discovered by pressing tab
after typing `rep.`.  IPython should auto-complete the list.

In general `Rep` has 2 interfaces: function based and class based. All the
work is done in the `rep.Rep` class and module-level functions are just
convenient proxies.


Example
--------

```
rep.replicate_1_to_n_and_compare(n=2, cycles=3, num=10, normal=False)

configuration:
   cluster_branch = None
   cluster_repo = None
   cluster_reset_data = True
   cluster_settings = None
   cluster_tmpdir = None
   connection_timeout = 30000
   create_target = False
   http_connections = 20
   prefix = cdyno
   server_url = http://adm:pass@localhost:15984
   source_url = None
   target_url = None
   timeout = 0
   worker_processes = 4
...
```

It:
 * Creates 3 databases: 1 (source), 2 and 3 (targets).
 * Sets up 2 continuous (normal=False) replications: 1->2 and 1->3.
 * Fills db 1 with 10 documents.
 * Polls target databases waiting for changes to get there.
 * It repeats last 2 steps 3 times in a row.


