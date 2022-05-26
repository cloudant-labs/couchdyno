Dyno
====

Utility which updates a some number of documents in a database during each
execution cycle. This can be used to model a database which gets continuously
updated over days, weeks, months, etc.

This was originally used to test a backup system, but it can also be used for a
simple db benchmarks or testing.


 * Pre-requisites: python 3.7+ & pip.

 * `./build.sh` :
   - Creates a local venv
   - Installs requirements
   - Installs couchdyno scripts in ./venv/bin

 * `./venv/bin/couchdyno-setup [options...] DB` to set up

 * `./venv/bin/couchdyno-execute DB` to execute one update cycle
   (or use with cron to run periodically, see section below)

 * `./venv/bin/couchdyno-info DB` to inspect latest stats

Note: DB can include a username and password

`couchdyno-setup` : sets up a database, and records a few parameters in it such
as:

  * `-t` | `--total` : total (max) number of documents
  * `-s` | `--size` : approx size of each document in bytes.
  * `-u` | `--update-per-run` : how many documents to update on each run.
  * `-w` | `--wait-to-fill` : after setup, fill database with documents It also

It also takes a `-f` | `--force` parameter which will delete and re-create the
database. By default if a database is already created, this script will show an
error.

`couchdyno-execute` : reads the parameters from the database and
updates `update-per-run` number of documents in it. For ex.: if
database contains documents 0,1,2,3,4, and `update-per-run=3`,
on first run it will update [0,1,2], then [3,4,0], then [1,2,3] etc.

`couchdyno-execute` can optinally run continuously using `-c` | `--continuous`
<seconds> option, which will keep running the execute code in an infinite loop
with <seconds> sleep in between cycles.

To override the number of documents update in each cycle use the `-u` |
`--update-per-run` parameter.


Examples
--------

```
./venv/bin/couchdyno-setup http://adm:pass@localhost:15984/db1
dyno_config:
    created : 1653543334 (2022-05-26T05:35:34)
    last_dt : 0
    last_errors : 0
    last_ts : 0 (1970-01-01T00:00:00)
    last_updates : 0
    size : 1000
    start : 0
    total : 1000
    updates : 10
    version : 1

./venv/bin/couchdyno-execute http://adm:pass@localhost:15984/db1
before:
  total: 1000
  size: 1000
  start: 0
  updating: 10

after:
  updates: 10
  dt (sec): 0.078
  rate (/sec): 128

new_state:
  created: 1653544672 (2022-05-26T05:57:52)
  last_dt: 0
  last_errors: 0
  last_ts: 1653544694 (2022-05-26T05:58:14)
  last_updates: 10
  size: 1000
  start: 10
  total: 1000
  updates: 10
  version: 1
```


Use With Cron
-------------

(This was tested using user's crontab on Mac OS X)

cron can be used to schedule execution of commands at different times.
Ex.: 1st minute of the hour, every day, 1st day of the year, at reboot, etc.

If enabled, regular users can run commands using cron as well. For that
use these commands:

 * crontab -e : edit crontab file, when saved, the new time schedule will
take effect. It is helpful to change the default editor to something else
before running sometimes. Like this: `$ export EDITOR=emacs && crontab -e`

 * crontab -l : lists users' current crontab file

 * crontab -r : clears all users' cron jobs

See man page or Google for crontab format. To inspect execution of the
command can pipe output to a log file by appending  >> <pathtofile>
at the end of command.

Example of crontab entries:

Run every day:

```
@daily <pathto>/couchdyno-execute <dburl>
```

Every minute:

```
* * * * * <pathto>/couchdyno-execute <dburl>
```

10pm on weekdays only:

```
0 22 * * 1-5  <pathto>/couchdyno-execute <dburl>
```
