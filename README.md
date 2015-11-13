Dyno
=====


What
----

Utility that updates a configured number of documents in a database
during each execution cycle.


Why
---

Need to model a database that gets continuously updated,
for ex.: a % of documents get modified every day, which then get
backup up or replicated.


How
---

 * Prerequisites: python 2.7+, virtualenv, and pip.
 * run ./build.sh which :
   - Creates a local virtualenv environment ./venv
   - Installs requirements
   - Install dyno scripts in ./venv/bin
 * run ./venv/bin/dyno-setup [options...] DBURL to set up
 * run ./venv/bin/dyno-execute DBURL to execute one update cycle
   (or put this in cron ^ to run periodically, see section below on that)
 * run ./venv/bin/dyno-info DBURL to inspect latest stats

Note: DBURL should include username and password


Details
-------

`dyno-setup` : sets up a database, and records a few
  parameters in it such as:
  * -t|--total : total number of documents how many test
        documents total should be in the database.
  * -s|--size : approx size of each document in bytes.
  * -u|--update-per-run : how many documents to update on each run.

It also takes a -f|--force parameter that will delete and
re-create the database. By default if a database is already created,
this script will show an error.

`dyno-execute` : reads the parameters from the database and
updates `update-per-run` number of documents in it. Both querying
for existing revision, and updating is done in batches on a
continguous range of documents. So, if database contains
documents 0,1,2,3,4, and `updates-per-run=3`, on first run
it will update [0,1,2], then [3,4,0], then [1,2,3] etc.

`dyno-execute` has a -c <seconds> parameter for debugging
where it will block and sleep that many seconds then execute
again forever. See section below on how to use cron to
setup periodic runs of dyno-execute.


Examples
--------

```$ ./venv/bin/dyno-setup https://btst:{pass}@btst.cloudant.com/cdyno1
Saved configuration:
 . created : 2015-11-13T18:48:02
 . history : (0 items)
 . last_dt : 0
 . last_errors : 0
 . last_ts : 0 (1970-01-01T00:00:00)
 . last_updates : 0
 . size : 1000
 . start : 0
 . total : 1000
 . updates : 10
 . version : 1
 run dyno-execute to start updating documents.```

After this cdyno1 data will contain a single metadata document which
saves all the parameters (size=1000B, total=1000 docs, on each run
update 10 of them).

```$ ./venv/bin/dyno-setup https://btst:{pass}@btst.cloudant.com/cdyno1
ERROR: db https://btst:{pass}@btst.cloudant.com/cdyno1 already exists
 To force reset it, use -f|--force```

It won't work running second time, as it will try to avoid accidentally
removing an existing database. Must use --force to override:


```$ ./venv/bin/dyno-execute https://btst:{pass}@btst.cloudant.com/cdyno1
Updating docs. Configuration:
 . created : 2015-11-13T18:48:02
 . history : (0 items)
 . last_dt : 0
 . last_errors : 0
 . last_ts : 0 (1970-01-01T00:00:00)
 . last_updates : 0
 . size : 1000
 . start : 0
 . total : 1000
 . updates : 10
 . version : 1
0 revs in 0.018 sec, @ 0 revs/sec
updated 10 docs: dt: 0.054 sec, @ 18434 docs/sec
New state:
 . created : 2015-11-13T18:48:02
 . history : (1 items)
 . last_dt : 0
 . last_errors : 0
 . last_ts : 1447440611 (2015-11-13T18:50:11)
 . last_updates : 10
 . size : 1000
 . start : 10
 . total : 1000
 . updates : 10
 . version : 1```

execute first prints the configuration it found, does its work. Then
updates configuration with statistics, and prepares it for the next run.

How To Use With Cron
--------------------

(This was tested using user's crontab on Mac OS X)

`cron` can be used to schedule execution of commands at different times.
Ex.: 1st minute of the hour, every day, 1st day of the year, at reboot, etc.

If enabled, regular users can run commands using cron as well. For that
use these commands:

 * `crontab -e` : edit crontab file, when saved, the new time schedule will
take effect. It is helpful to change the default editor to something else
before running sometimes. Like this:`$ export EDITOR=emacs && crontab -e`

 * `crontab -l` : lists users' current crontab file

 * `crontab -r` : clears all users' cron jobs

See man page or Google for crontab format. To inspect execution of the
command can pipe output to a log file by appending  ``>> <pathtofile>``
at the end of command.

Example of crontab entries:

Run `dyno-execute` every day:
```@daily ./dyno/venv/bin/dyno-execute https://btst:{pass}@btst.cloudant.com/cdyno1 >> /tmp/dyno1```

...every minute:
```* * * * * ./dyno/...```

...10pm on weekdays only:
```0 22 * * 1-5 ./dyno/...```
