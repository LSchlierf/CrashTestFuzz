# How to run CrashTestFuzz on a newly compiled version of CedarDB

## Step 0: How to run CrashTestFuzz on the latest public CedarDB version

We do this to make sure that CrashTestFuzz runs with a working configuration.

Install dependencies:

```sh
sudo apt update && sudo apt install python3 python3-dev libpq-dev

pip install psycopg2 requests
```

Run on the latest CedarDB version:

```sh
python3 main.py -x cedardb-configs/test-cedardb-latest.json -r 0
```

## Step 1: Link CedarDB

The test process works by building a docker container with
- the newly compiled cedadb binary
- lazyfs
- all dependencies

To do this, the CedarDB binary must be in `SUT/cedardb-local/docker`.

To achieve this, you can **hard**-link your build output:

```sh
ln ../wherever/you/have/your/build/cedardb SUT/cedardb-local/docker/cedardb
```

## Step 2: Run CrashTestFuzz

```sh
python3 main.py -x cedardb-configs/test-cedardb-local.json -r 0
```

This will take a while (>10m).

## Step 3: Interpreting the output

### Files and locations

After your run has finished, CrashTestFuzz will have generated reports for the test runs. These are located at `logs/cedardb-local/test-[timestamp]/[seed]/visualization`.

Here you will find one directory for each test outcome that has occured. These can be:

- `initial-success`: Generally uninteresting, this means that CedarDB didn't crash during the workload execution. This usually happens for higher hurdles.
- `correct-content`: This is what we are aiming for.
- `correct-content; lost-commit`: This is also ok. This means that CedarDB crashed during the execution of a `COMMIT` command, didn't `ACK` the command, and didn't persist the corresponding transaction, which is correct behavior.
- `correct-content; unconfirmed-commit`: This is also ok. This means that CedarDB crashed during the execution of a `COMMIT` command, didn't `ACK` the command, but persisted the corresponding transaction, which can also be considered correct behavior. So far only observed for PostgreSQL.
- `correct-parent-content`: This means that CedarDB crashed during recovery (or at least before the workload could start), but reproduced the (correct) last known state of the database.
- `incorrect-parent-content`: This means that CedarDB crashed during recovery (or at least before the workload could start), and additionally reproduced an incorrect content, that differs from the last known state of the db.
- `incorrect-content`: This means that CedarDB reproduced an incorrect content after recovery.
- `incorrect content; lost-commits: [n]`: This means that CedarDB reproduced an old known state of the db and lost the `n` last confirmed `COMMIT`s. So far only observed using async commits (currently disabled via ENV var in Dockerfile).

Inside these directories, you can find the generated HTML reports. These are named after their index. This works as follows:

```
Workload run hurdle index (at which point the filesystem crashes):
0 -> 0-(slim|wide).json
1 -> 1-(slim|wide).json
2 -> 2-(slim|wide).json
...
```

These are the files for depth 1. 

Every container (except `initial-success` amd `incorrect-content` ones) are duplicated after their workload run, and recovery and another workload is run on them for depth 2:

```
0 -> 0-(slim|wide).json
|- 0 -> 0-0-(slim|wide).json
|- 1 -> 0-1-(slim|wide).json
|- 2 -> 0-2-(slim|wide).json
...
1 -> 1-(slim|wide).json
|- 0 -> 1-0-(slim|wide).json
|- 1 -> 1-1-(slim|wide).json
|- 2 -> 1-2-(slim|wide).json
...
2 -> 2-(slim|wide).json
|- 0 -> 2-0-(slim|wide).json
|- 1 -> 2-1-(slim|wide).json
|- 2 -> 2-2-(slim|wide).json
...
```

... and so on, until recursion depth is reached.

These correspond to the thread ids you might see during test execution.

### Reading a report

#### Header Info

Every report has a header with some fundamental information:
- Workload information: The basic parameters of the SQL workload
- Test information (per depth level): The parameters for when lazyfs should crash + result for that depth
- (Sometimes) Trace information: If the container started and executed (part of) a workload, this section shows some information on the executed transaction trace, including a hash. This hash is the same for the same workloads, since logs, timestamps etc are filtered out before hashing. This is especially useful for comparing transaction traces.

#### Trace table

This table shows all transactions with each action that happened on them in order. These actions (BEGIN, INSERT, UPDATE, DELETE, ROLLBACK, COMMIT) are well-ordered, i.E. they always get executed in the same order for the same seed, although on different (usually parallel) transactions.

**Top bar of the table**: This shows the index of the transaction

**Left side of the table**: Every action on a transaction (INSERT, UPDATE, DELETE) has a number that is unique to that depth. These are shown on the left.

**Right side of the table**: This side show the collected logs from cedardb and lazyfs in dropdown menus, matched to the correct action that they occured after.

Above the table, there is an additional drop down menus with the logs from before the workload execution started (`initial log`) and during the recovery of the verification duplicate (`restartLog`).

#### Mismatch table

The most interesting reports are `incorrect-content` and `incorrect-parent-content`. These feature an additional part in their report; the mismatch table. This is a dropdown that compares the should-be-state and is-state of the db after recovery.

Data items that are only in one of these states are marked in red.

The data items are 4-tuples (`a`,`b`,`c`,`d`), with each part having semantic meaning:
- `a`: the size of the table before its insertion (note snapshot isolation)
- `b`: the action (comparable to LSN), see left side of the transaction trace table
- `c`: the transaction ID, see top bar of the table
- `d`: the depth, basically the index of the transaction trace table, starting at the top with 0.

Reading these tuples allows you to match a tuple to the action and transaction where it was inserted / last modified in order to assess whether a winner transaction was lost, a loser transaction was persisted, or something else happened.

## Misc

### Frequent Errors (and how to fix them)

Sometimes CrashTestFuzz crashes or spits out an error in the shape of "another instance of CrashTestFuzz is running".

Make sure that no other instance of CrashTestFuzz is running on your machine. CrashTestFuzz uses a tmpfs to store all the container data at `/dev/shm/ctf/[SUT]`, which gets symlinked to the correct SUT directory, in this case `cedardb-local`.

If no other instance is running:
- Clean up all orphaned docker containers (will always have a name in the shape of `lazycedardb-local-[uuid]`)
- Delete the docker image `lazycedardb-local`
- Delete the directory `SUT/cedardb-local/container` and all its contents
- Delete the directory `/dev/shm/ctf` and all its contents

Then try again.

### Other Resources

[Main README](./README.md)

[CrashTestFuzz Thesis](https://talks.db.in.tum.de/uploads/67/fb27c88359422cbb4df1227a627ef0/thesis.pdf)

[Lucas :)](https://cedardbworkspace.slack.com/team/U0ASKCDM475)
