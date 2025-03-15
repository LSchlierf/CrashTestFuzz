# DBHWBench

A fuzzer for DBMS data loss bugs resulting from a file system failure.

Filesystem failures are simulated using [LazyFS](https://www.github.com/dsrhaslab/lazyfs).

## Installation

### Dependencies

```sh
sudo apt update && sudo apt install python3 python3-dev libpq-dev

pip install psycopg2
```

## Usage

DBHWBench can

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

DBHWBench can export the generated `.json` test results from a single container, both as a `.html` page as well as a perfetto `.trace` file (open with [ui.perfetto.dev](https://ui.perfetto.dev/)).

```sh
./exportVisualization.py path/to/result1.json path/to/result2.json ...
```

The generated files are placed next to the given `.json` files.

## Contact

I can be contacted via email: [LucasSchlierf@gmail.com](mailto:lucasschlierf@gmail.com).
