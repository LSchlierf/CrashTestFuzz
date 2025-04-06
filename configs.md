# Configuration

The following document lists all current Parameters for CrashTestFuzz, including their json key as well as command line argument name.

JSON configurations can be passed using `./main.py -x ./path/to/config.json`. Command line arguments override config values.

## Parameters

| json key                     | json data type | command line argument              | meaning                                            | notes                                                   |
|-|-|-|-|-|
| `verbose`                    | int            | `-v`,`--verbose`, additive         | debug level                                        | theoretical max 4, usable until 2                       |
| `seed`                       | list of int    | `-r`,`--seed`,`--randseed`         | RNG seed                                           | overrides `from`,`until`                                |
| `from`                       | int            | `-f`,`--from`                      | seed range start (inclusive)                       |                                                         |
| `until`                      | int            | `-u`,`--until`                     | seed range end (exclusive)                         | default `10_000`                                        |
| `sut`                        | string         | `--sut`                            | System under Test                                  |                                                         |
| `sync_method`                | string         | `-m`,`--sync-method`               | `WAL_SYNC_METHOD` passed to SUT                    |                                                         |
| `num_transactions`           | int            | `--num-transactions`               | number of transactions                             |                                                         |
| `concurrent_connections_avg` | float          | `--concurrent-transactions-avg`    | number of concurrent transactions (avg)            |                                                         |
| `concurrent_connections_var` | float          | `--concurrent-transactions-var`    | number of concurrent transactions (var)            |                                                         |
| `transaction_size_avg`       | float          | `--transaction-size-avg`           | number of statements per transaction (avg)         |                                                         |
| `transaction_size_var`       | float          | `--transaction-size-var`           | number of statements per transaction (var)         |                                                         |
| `statement_size_avg`         | float          | `--statement-size-avg`             | number of data items per statement (avg)           |                                                         |
| `statement_size_var`         | float          | `--statement-size-var`             | number of data items per statement (var)           |                                                         |
| `p_commit`                   | float          | `--p-commit`                       | probability for commit                             | `p_rollback` = 1 - `p_commit`                           |
| `p_insert`                   | float          | `--p-insert`                       | probability for insert                             |                                                         |
| `p_update`                   | float          | `--p-update`                       | probability for update                             | `p_delete` = 1 - (`p_insert` + `p_update`)              |
| `p_serialization_failure`    | float          | `--p-serialization-failure`        | probability for concurrency conflict               | target value only, cc only when locked values available |
| `concurrent`                 | int            | `-c`,`--concurrent`                | number of concurrent threads, each with one SUT    |                                                         |
| `log`                        | string         | `-l`,`--log`                       | log output level                                   | `all`/`failed`/`retry`(only for `verify`==True)/`none`  |
| `verify`                     | bool           | `--verify`, stores `True`          | verification run                                   | = no fault injection                                    |
| `steps`                      | int            | `-s`,`--steps`                     | number of subdivisions for fault injection hurdles | only for `verify`==False                                |
| `walfile`                    | list of string | `-w`,`--walfile`                   | target file for fault injection                    | only for `verify`==False                                |
| `operation`                  | list of string | `-o`,`--operation`                 | target operation for fault injection               | only for `verify`==False                                |
| `timing`                     | list of string | `-t`,`--timing`                    | timing for fault injection                         | only for `verify`==False                                |
| `recursion_depth`            | int            | `-d`,`--recursion-depth`           | number of restarts with fault injection            | only for `verify`==False                                |
| `recursion_factor`           | float          | `-q`,`--recursion-factor`          | branching degree for `steps` parameter             | only for `verify`==False                                |
| `checkpoint`                 | bool           | `-k`,`--checkpoint`, stores `True` | `fsync()` on everything after finished transaction | only for `verify`==False                                |

See [`demo-configs`](demo-configs) and [`knonwn-bugs`](known-bugs) for example configurations, use `./main.py -h` / `./main.py --help` for more information.
