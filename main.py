#!/usr/bin/env python
import argparse
import benchmark
import json
import os
import shared
import utils

def main():
    p = argparse.ArgumentParser(prog="DBHWBench", formatter_class=argparse.RawTextHelpFormatter)
    
    p.add_argument("-v", "--verbose", action="count", help=f"Increases the verbosity level (max 4, default {shared.DEBUG_LEVEL})")
    
    p.add_argument("-r", "--seed", "--randseed", help="Seed(s) for the transaction trace, can be passed multiple times", action="append", type=int)
    p.add_argument("-f", "--from", help="Starting seed for the transaction traces (inclusive)", type=int)
    p.add_argument("-u", "--until", help="End seed for the transaction traces (exclusive, default 10_000)", type=int)
    
    p.add_argument("-c", "--concurrent", help=f"Number of concurrent tests to run (default {shared.CONCURRENT_TESTS})", type=int)
    p.add_argument("-s", "--steps", type=int)
    p.add_argument("-l", "--log", choices=["none", "retry", "failed", "all"], help="If specified, will generate a log for the respective transaction trace\n-> none:\tdon't log anything\n-> retry:\tretry failed traces with logging\n-> failed:\tlog everything, discard logs for successful traces\n-> all:\t\tlog all")
    
    p.add_argument("--verify", action="store_const", const=True, default=None, help="If specified, will verify the seed(s), running without injected hardware faults")
    p.add_argument("--sut", choices=os.listdir("SUT"), help="System Under Test")
    p.add_argument("-w", "--walfile") # TODO: automatic selection for each sut
    p.add_argument("-t", "--timing")
    p.add_argument("-o", "--operation")
    p.add_argument("-m", "--sync-method", choices=shared.SYNC_METHODS)
    p.add_argument("-k", "--checkpoint", action="store_const", const=True, default=None)
    
    p.add_argument("--num-transactions", type=int)
    p.add_argument("--concurrent-transactions-avg", type=float)
    p.add_argument("--concurrent-transactions-var", type=float)
    p.add_argument("--transaction-size-avg", type=float)
    p.add_argument("--transaction-size-var", type=float)
    p.add_argument("--statement-size-avg", type=float)
    p.add_argument("--statement-size-var", type=float)
    p.add_argument("--p-commit", type=float)
    p.add_argument("--p-insert", type=float)
    p.add_argument("--p-update", type=float)
    p.add_argument("--p-serialization-failure", type=float)
    
    p.add_argument("-x", "--config", metavar="FILE.json", help="config file that holds parameters.\nCan be overridden by cmd line args.")
    
    n = p.parse_args()
    
    if n.config is not None:
        if os.path.exists(n.config) and os.path.isfile(n.config):
            with open(n.config) as f:
                data = json.load(f)
            setConfigFileValues(n, data)
        else:
            utils.error("Invalid config file", kill=True)
    
    if n.until == None:
        n.until = 10_000
    if n.verbose == None:
        n.verbose = 0
    
    print(n)
    
    setSharedValues(n)
    
    assert shared.NUM_TRANSACTIONS > 0
    assert shared.P_COMMIT <= 1 and shared.P_COMMIT > 0
    assert shared.P_INSERT > 0
    assert shared.P_UPDATE > 0
    assert (shared.P_INSERT + shared.P_UPDATE) <= 1
    assert shared.P_SERIALIZATION_FAILURE < 1 and shared.P_SERIALIZATION_FAILURE >= 0
    
    if n.seed != None:
        if n.verify:
            utils.info(f"Verifying seed{'s' if len(n.seed) > 1 else ''}", *n.seed)
            benchmark.verifySeedsThreaded(n.log, seeds=n.seed)
        else:
            utils.info(f"Running seed{'s' if len(n.seed) > 1 else ''}", *n.seed)
            benchmark.runSeedsThreaded(n.log, seeds=n.seed)
    elif getattr(n, "from") != None:
        if getattr(n, "from") >= n.until:
            p.print_help()
            utils.error("end of seed range must be at least 1 above start", kill=True)
        if n.verify:
            utils.info("Verifying seeds", getattr(n, "from"), "through", n.until - 1)
            benchmark.verifySeedsThreaded(n.log, [i for i in range(getattr(n, "from"), n.until)])
        else:
            utils.info("Running seeds", getattr(n, "from"), "through", n.until - 1)
            benchmark.runSeedsThreaded(n.log, [i for i in range(getattr(n, "from"), n.until)])
    else:
        if n.verify:
            utils.info("Verifying random seed")
            raise NotImplementedError
            #TODO
        else:
            utils.info("Running random seed")
            raise NotImplementedError
            #TODO
            
    utils.info("All done.")

def setConfigFileValues(n, data):
    for key in data:
        if getattr(n, key) == None:
            setattr(n, key, data[key])

def setSharedValues(n):
    if n.sut is not None:
        shared.SUT = n.sut
    if n.sync_method is not None:
        shared.SYNC_METHOD = n.sync_method
    if n.verbose != 0:
        shared.DEBUG_LEVEL = n.verbose
    if n.concurrent is not None:
        shared.CONCURRENT_TESTS = n.concurrent
    if n.walfile is not None:
        shared.FILE = n.walfile
    
    if n.operation is not None:
        shared.OP = n.operation
    if n.timing is not None:
        shared.TIMING = n.timing
    if n.steps is not None:
        shared.STEPS = n.steps
    if n.checkpoint is not None:
        shared.CHECKPOINT = n.checkpoint
    
    if n.num_transactions is not None:
        shared.NUM_TRANSACTIONS = n.num_transactions
    if n.p_commit is not None:
        shared.P_COMMIT = n.p_commit
    if n.p_insert is not None:
        shared.P_INSERT = n.p_insert
    if n.p_update is not None:
        shared.P_UPDATE = n.p_update
    if n.p_serialization_failure is not None:
        shared.P_SERIALIZATION_FAILURE = n.p_serialization_failure
    
    (ctAvg, ctVar) = shared.CONCURRENT_TRANSACTIONS
    if n.concurrent_transactions_avg is not None:
        ctAvg = n.concurrent_transactions_avg
    if n.concurrent_transactions_var is not None:
        ctVar = n.concurrent_transactions_var
    shared.CONCURRENT_TRANSACTIONS = (ctAvg, ctVar)
    
    (tsAvg, tsVar) = shared.TRANSACTION_SIZE
    if n.transaction_size_avg is not None:
        tsAvg = n.transaction_size_avg
    if n.transaction_size_var is not None:
        tsVar = n.transaction_size_var
    shared.TRANSACTION_SIZE = (tsAvg, tsVar)
    
    (ssAvg, ssVar) = shared.STATEMENT_SIZE
    if n.transaction_size_avg is not None:
        ssAvg = n.transaction_size_avg
    if n.transaction_size_var is not None:
        ssVar = n.transaction_size_var
    shared.STATEMENT_SIZE = (ssAvg, ssVar)

if __name__ == "__main__":
    main()
