# jerbs

Command-line work-stealing scheduler.

## Installation

If you use the **Nix** package manager, there's a package in [my
overlay](https://github.com/kazcw/phoe.nix).

**Otherwise**, you can build `jerbs` with cargo:
```
cargo install jerbs
```

## Operation

Create a job database:
```
$ jerbs work.db init
```

Define a job and enqueue some repetitions:
```
$ jerbs work.db create --count 17 --data "info for thing to do 17 times"
1
```
The output is the job id, which you can use to edit the job later.

See what's scheduled:
```
$ jerbs work.db list-available -v
1       17      "info for thing to do 17 times"
```
(Note: do not use verbose output (`-v`) for scripting. It is intended to be
human-readable and the format is unstable.)

Run a worker:
```
$ while jerbs work.db take $$ | read JOB; do echo $JOB; done
```
Now start some more!

## Typical Usage

I made this so I could have a tmux with a worker process in each pane, all
taking jobs from the same queue. The worker processes run a shell script that
uses this utility to pick the next job.

A job's payload is a blob of data. What's in the blob is up to you. If a job
needs multiple parameters, the blobs could be filenames indicating where to
find the job data; or, you might pack the data directly into the blob with a
delimiter-based format or `jq` or something.

Worker IDs can be any utf-8 string. If your worker is a bash script, you can
pass `$$` to use your worker's PID.

Because the data blob for your task may contain characters that are subject to
string interpolation hazards, any command that requires a blob will read it
from standard input by default. If your blobs are shell-safe, you can instead
use `--data` to include your blob in the arguments.

## Comparison to alternatives

Other work-stealing schedulers (like GNU Parallel) are frameworks; they own the
worker processes, so you can only configure workers through the framework.
`jerbs` inverts this paradigm: `jerbs` is a utility to be used from your worker
script. With `jerbs` you can easily assign unique resources to the workers, pin
workers to CPUs/NUMA nodes, or dynamically vary the number of simultaneous
jobs. At last, the workers control the means of production.
