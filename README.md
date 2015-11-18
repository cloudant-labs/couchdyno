Dyno
=====

Utility which updates a configured number of documents in a database
during each execution cycle. This can be used to model a database
which gets continuously updated over days, weeks, months, etc.


Usage
-----

 * Pre-requisites: python 2.7, virtualenv, pip.

 * `./build.sh` :
   - Creates a local virtualenv environment ./venv
   - Installs requirements
   - Installs dyno scripts in ./venv/bin

 * `./venv/bin/dyno-setup [options...] DB` to set up

 * `./venv/bin/dyno-execute DB` to execute one update cycle
   (or use with cron to run periodically, see section below)

 * `./venv/bin/dyno-info DB` to inspect latest stats

Note: DB can include username and password


Details
-------

dyno-setup : sets up a database, and records a few
parameters in it such as:

  * -t | --total : total number of documents, i.e. how many test
        documents total should be in the database.
  * -s | --size : approx size of each document in bytes.
  * -u | --update-per-run : how many documents to update on each run.
  * -w | --wait-to-fill : after setup, fill database with documents
It also takes a -f | --force parameter which will delete and
re-create the database. By default if a database is already created,
this script will show an error.

dyno-execute : reads the parameters from the database and
updates update-per-run number of documents in it. For ex.: if
database contains documents 0,1,2,3,4, and update-per-run=3,
on first run it will update [0,1,2], then [3,4,0], then [1,2,3] etc.

dyno-execute can optinally run continuously using
-c | --continuous <seconds> option, which will keep running the
execute code in an infinite loop with <seconds> sleep in between cycles,

It also can override the number of documents to be updated per each
cycles using the -u | --update-per-run parameter.


Examples
--------

```
$ ./venv/bin/dyno-setup https://btst:{pass}@btst.cloudant.com/cdyno1
```

```
Saved configuration:
 . created : 1447653768 (2015-11-16T06:02:48)
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
run dyno-execute to start updating documents.
```

After this cdyno1 data will contain a single metadata document which
saves all the parameters (size=1000B, total=1000 docs, on each run
update 10 of them).


```
$ ./venv/bin/dyno-execute https://btst:{pass}@btst.cloudant.com/cdyno1
```

```
Total docs: 1000  doc size: 1000  start: 0
Updating 10 docs in batches of 2000
updated 10 docs: dt: 0.101 sec, @ 99 docs/sec
New state:
 . created : 1447653768 (2015-11-16T06:02:48)
 . history : (1 items)
 . last_dt : 0
 . last_errors : 0
 . last_ts : 1447653777 (2015-11-16T06:02:57)
 . last_updates : 10
 . size : 1000
 . start : 10
 . total : 1000
 . updates : 10
 . version : 1
```


Use With Cron
--------------------

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
@daily <pathto>/dyno-execute <dburl>
```

Every minute:

```
* * * * * <pathto>/dyno-execute <dburl>
```

10pm on weekdays only:

```
0 22 * * 1-5  <pathto>/dyno-execute <dburl>
```
