# CrashTestFuzz

A fuzzer for DBMS data loss bugs resulting from a file system failure.

Filesystem failures are simulated using [LazyFS](https://www.github.com/dsrhaslab/lazyfs).

## Installation

### Dependencies

```sh
sudo apt update && sudo apt install python3 python3-dev libpq-dev

pip install psycopg2
```

Also make sure your local user can issue docker commands.

## Usage

CrashTestFuzz can

- Verify its own logic against a DBMS
- Test a DBMS using simulated fs failures
- Visualize test results

Use ```main.py -h``` for detailed description of the arguments.

### Verification

```sh
./main.py -x configs/verify-generic.json --sut postgres -r 0
```

### Testing a DBMS

Example for postgres:

```sh
./main.py -x configs/test-postgres.json -r 0
```

See the `configs` folder for more examples.

### Exporting test results

CrashTestFuzz can export the generated `.json` test results from a single container, both as a `.html` page as well as a perfetto `.trace` file (open with [ui.perfetto.dev](https://ui.perfetto.dev/)).

```sh
./exportVisualization.py path/to/result1.json path/to/result2.json ...
```

The generated files are placed next to the given `.json` files. If the `.json` file is in the `logs/thesut/...` folder, the script will automatically detect the proper SUT, otherwise you will be prompted to enter the SUT name.

## Adding your own SUT

CrashTestFuzz is extensible with further SUTs. The following section descibes the requirements to add your own SUT.


### Docker image etc.

Your SUT should run inside a Docker container. The SUT folder should be made in the following structure:

```
SUT
|-- yoursutname
    |
    |--scripts
    |   |-- build-image.sh
    |   |-- cleanup-all.sh
    |   |-- cleanup-env.sh
    |   |-- cleanup-envs.sh
    |   |-- prep-env.sh
    |   |-- run-container.sh
    |   |-- stop-contaienr.sh
    |   |-- stop-sut.sh
    |
    |-- container
        |-- container-0123456789abcdef
            |-- faults.fifo
            |-- lazyfs.log
            |-- yoursutname.log
            |-- persisted
                |-- ...
```

These scripts should:

- **build-image.sh:** build your docker image. Can take parameter WAL_SYNC_METHOD
- **prep-env.sh:** prepare the host environment for a single container: make a folder ("container-thecontainerid") with the lazyfs fifo ("faults.fio"), the lazyfs log target file ("lazyfs.log"), your sut log target file ("yoursutname.log") and the persisted lazyfs storage ("persisted"). Takes a container id.
- **run-container.sh:** start the specified container (might not be the first start). The files specified for `prep-env.sh` should be mounted to the respective in-container counterpart using `docker run ... -v ./../container/container-$CONTAINER_ID/persisted:/tmp/lazyfs.root` for example. Takes the container id, port (may be 0, letting docker decide the port), and crash cmd to append to the lazyfs config before starting lazyfs (may be empty).
- **stop-sut.sh:** stop the sut inside the container (without stopping lazyfs). Takes the container id
- **stop-container.sh:** stop the container (optionally stopping the sut and lazyfs before). Takes the container id
- **cleanup-env.sh:** clean up the container directory of a stopped container. Takes the container id
- **cleanup-envs.sh:** stop all containers, remove them, and clean up all container direcotries.
- **cleanup-all.sh:** stop all containers, remove them, clean up all container directories and remove the docker image.

Should anything be unclear, check out the `SUT/postgres` folder.

### Code considerations

Should your SUT log with timestamps, consider teaching CrashTestFuzz how to read them by amending the function `suttimestamp` in `utils.py`. This will enable log merging, otherwise all log lines will be interpreted as having timestamp 0, meaning they will be at the very beginning of everything.

If you want to use the cmd line option `--walfile auto`, consider adding the path of the WAL-file for your SUT to the dict `WAL_FILES` in `main.py`. The file path should be qualified from the directory that lazyfs mounts to.

## Contact

I can be contacted via email: [LucasSchlierf@gmail.com](mailto:lucasschlierf@gmail.com).
