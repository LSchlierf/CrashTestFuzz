import itertools
import json
import shared
import shutil
from threading import Thread
from utils import *
from uuid import uuid4
from visualization import makeHTMLPage, makeTrace

SYNC_METHODS = [
    "none", "open_dsync", "open_sync", "fdatasync", "fsync"
]

test_db = {
    "name": shared.DB_TABLENAME,
    "schema": [("a", "int"), ("b", "int")]
}

def copyLogs(seed, id, success):
    shutil.copy(os.path.abspath(f"SUT/{shared.SUT}/container/container-{id}/{shared.SUT}.log"), os.path.abspath(f"logs/{shared.SUT}/{shared.TEST_RUN}/{seed}/{'s' if success else 'f'}-{id}-{shared.SUT}.log"))
    shutil.copy(os.path.abspath(f"SUT/{shared.SUT}/container/container-{id}/lazyfs.log"), os.path.abspath(f"logs/{shared.SUT}/{shared.TEST_RUN}/{seed}/{'s' if success else 'f'}-{id}-lazyfs.log"))

def logAll(seed, id, metadata, log):
    if not os.path.exists("logs/" + shared.SUT + "/" + shared.TEST_RUN + "/" + str(seed)):
        os.makedirs("logs/" + shared.SUT + "/" + shared.TEST_RUN + "/" + str(seed), exist_ok=True)
    dumpIntoFile("logs/" + shared.SUT + "/" + shared.TEST_RUN + "/" + str(seed) + "/" + ("s-" if metadata["successful"] else "f-") + id + ".json", json.dumps({"metadata": metadata, "log": log}, indent=4))
    copyLogs(seed, id, metadata["successful"])

def runTestThreaded(batch, threadNumber, hurdle, seed, results, makeLog):
    tls.number = threadNumber + 1
    tls.batch = batch + 1
    debug("running test in thread", threadNumber + 1, "hurdle is", hurdle, level=2)
    try:
        if runTest(hurdle, seed, makeLog):
            results[batch * shared.CONCURRENT_TESTS + threadNumber] = True
        else:
            error("Test failed, seed", seed)
    except Exception as e:
        error("Test failed with exception", type(e), e, "seed", seed)

def runTest(hurdle, seed, makeLog):
    cmd = "\n".join(["\n[[injection]]", "type = \"clear-cache\"", f"from = \"/tmp/lazyfs.root/{shared.FILE}\"", f"timing = \"{shared.TIMING}\"", f"op = \"{shared.OP}\"", f"occurrence = {hurdle}", "crash = true"])
    id = prepHostEnvironment()
    testMetadata = {
        "cmd": cmd,
        "hurdle": hurdle,
        "seed": seed,
        "targetFile": shared.FILE,
        "timing": shared.TIMING,
        "operation": shared.OP
    }
    if makeLog in ["all", "failed"]:
        (logPoll, logPipe) = openReader(id, "lazyfs")
    else:
        (logPoll, logPipe) = (None, None)
    port = runContainer(id, crashcmd=cmd)
    waitUntilAvailable(id, port, 90)
    create(shared.DB_TABLENAME, test_db["schema"], port)
    (content, metadata, log) = runWorkload(port, seed, makeLog=(makeLog in ["all", "failed"]), logPoll=logPoll, logPipe=logPipe)
    closeReader(logPipe)
    if metadata["successful"] and verify(shared.DB_TABLENAME, content, port):
        info("Test ran without faults")
        stopSUT(id)
    else:
        info("Test ran with faults")
    testMetadata["traceHash"] = traceHash(log)
    debug("trace hash:", testMetadata["traceHash"], level=1)
    try:
        stopContainer(id)
        port = runContainer(id)
        waitUntilAvailable(id, port, 90)
        if verify(shared.DB_TABLENAME, content, port):
            debug("Correct db content after restart", level=1)
            if makeLog == "all":
                testMetadata["result"] = "correct-content"
                metadata["testMetadata"] = testMetadata
                metadata["successful"] = True
                logAll(seed, id, metadata, log)
            cleanupContainer(id)
            return True
        else:
            error("Incorrect db content after restart")
            if makeLog in ["all", "failed"]:
                testMetadata["result"] = "incorrect-content"
                metadata["testMetadata"] = testMetadata
                metadata["successful"] = False
                logAll(seed, id, metadata, log)
            cleanupContainer(id)
            return False
    except:
        error("Container didn't restart")
        if makeLog in ["all", "failed"]:
            testMetadata["result"] = "no-restart"
            metadata["testMetadata"] = testMetadata
            metadata["successful"] = False
            logAll(seed, id, metadata, log)
        cleanupContainer(id)
        return False

def runSeedsThreaded(makeLog, start=0, end=10_000):
    
    shared.TEST_RUN = "test-" + str(uuid4())

    buildSUTImage()
    for seed in range(start, end):
        if os.path.exists(".terminate"):
            info("Terminating")
            os.remove(".terminate")
            break
        debug("seed", seed, level=1)

        results = [False for _ in range(shared.STEPS)]

        debug("running workload without injected faults", level=1)

        containerID = prepHostEnvironment()
        port = runContainer(containerID)

        waitUntilAvailable(containerID, port, timeout=90)
        create(shared.DB_TABLENAME, test_db["schema"], port=port)
        
        if makeLog in ["all", "failed"]:
            (poll, pipe) = openReader(containerID, shared.SUT)
        else:
            (poll, pipe) = (None, None)

        (content, metadata, log) = runWorkload(port, seed, makeLog in ["all", "failed"], poll, pipe)
        closeReader(pipe)
        
        if not metadata["successful"]:
            error("first run failed, seed", seed)
            if log in ["all", "failed"]:
                logAll(seed, containerID, metadata, log)
                cleanupContainer(containerID)
            continue

        stopSUT(containerID)

        lazyfsLogs = readLogs(containerID, "lazyfs")
        cleanupContainer(containerID)

        files = extractFiles(lazyfsLogs)
        if makeLog != "none":
            if not os.path.exists("logs/" + shared.SUT + "/" + shared.TEST_RUN + "/" + str(seed)):
                os.makedirs("logs/" + shared.SUT + "/" + shared.TEST_RUN + "/" + str(seed), exist_ok=True)
            dumpIntoFile("logs/" + shared.SUT + "/" + shared.TEST_RUN + "/" + str(seed) + "/testHurdles-" + containerID + ".json", json.dumps(files, indent=4), force=True)

        maxOps = 0
        for item in files:
            if shared.FILE in item and shared.OP in files[item]:
                maxOps = max(maxOps, files[item][shared.OP])

        stepSize = int(maxOps / shared.STEPS)

        debug("max number of ops is", maxOps, "step size is", stepSize, level=3)

        hurdles = [(i+1) * stepSize for i in range(shared.STEPS)]

        debug(len(hurdles), "hurdles:", hurdles, level=3)

        for (index, batch) in enumerate(itertools.batched(hurdles, shared.CONCURRENT_TESTS)):
            
            debug("running test batch", index + 1, level=1)
            threads = [Thread(target=runTestThreaded, args=(index, number, hurdle, seed, results, makeLog)) for (number, hurdle) in enumerate(batch)]
            
            for (n, t) in enumerate(threads):
                debug("starting thread", n + 1, level=2)
                t.start()
            for (n, t) in enumerate(threads):
                debug("joining thread", n + 1, level=2)
                t.join()

            debug("finished test batch", index + 1, level=1)
            cleanupEnvs()

        if not False in results:
            debug("all tests ran without errors.", level=1)
        else:
            error("Errors in tests", [n for (n, s) in enumerate(results) if not s], "with hurdles", [hurdles[i] for (i, s) in enumerate(results) if not s])
            if makeLog == "retry":
                for h in [hurdles[i] for (i, s) in enumerate(results) if not s]:
                    runTest(h, seed, "all")
            debug("seed was", seed, level=1)
        
        cleanupEnvs()

    cleanupAll()
    debug("all done.", level=1)

def verifySeedThreaded(batch, threadNumber, seed, results, makeLog):
    tls.number = threadNumber + 1
    tls.batch = batch + 1
    debug("verifying seed", seed, level=1)
    try:
        if verifySeed(seed, makeLog):
            results[threadNumber] = True
            debug("Success", level=1)
        else:
            error("Failure reported, seed", seed)
    except:
        error("Failure reported, seed", seed)

def verifySeed(seed, makeLog):
    containerID = prepHostEnvironment()
    if makeLog in ["all", "failed"]:
        (logPoll, logPipe) = openReader(containerID, "lazyfs")
    else:
        (logPoll, logPipe) = (None, None)
    port = runContainer(containerID)
    waitUntilAvailable(containerID, port, timeout=90)
    create(test_db["name"], schema=test_db["schema"], port=port)
    (content, metadata, log) = runWorkload(port, seed, makeLog=(makeLog in ["all", "failed"]), logPoll=logPoll, logPipe=logPipe)
    closeReader(logPipe)
    if makeLog == "all" or (makeLog == "failed" and not metadata["successful"]):
        logAll(seed, containerID, metadata, log)
        debug("trace hash:", traceHash(log), level=2)
    cleanupContainer(containerID)
    debug("seed was", seed, level=1)
    return metadata["successful"]

def verifySeedsThreaded(makeLog, start=0, end=10_000):

    buildSUTImage()

    for (i, b) in enumerate(itertools.batched(range(start,end), shared.CONCURRENT_TESTS)):
        results = [False for _ in range(len(b))]

        info("seeds", b[0], "through", b[-1])

        threads = [Thread(target=verifySeedThreaded, args=(i, t, s, results, makeLog)) for (t, s) in enumerate(b)]
        for (n, t) in enumerate(threads):
            debug("starting thread", n + 1, level=2)
            t.start()
        for (n, t) in enumerate(threads):
            debug("joining thread", n + 1, level=2)
            t.join()
        debug("finished test batch", i + 1, level=1)

        if not False in results:
            debug("all tests ran without errors.", level=1)
        elif makeLog == "retry":
            # debug(results, level=3)
            error("Errors in tests", [n for (n, s) in enumerate(results) if not s], "with seeds", [b[i] for (i, s) in enumerate(results) if not s])
            failed = [b[i] for (i, s) in enumerate(results) if not s]
            for seed in failed.copy():
                id = prepHostEnvironment()
                (logPoll, logPipe) = openReader(id, "lazyfs")
                port = runContainer(id)
                waitUntilAvailable(id, port, timeout=90)
                create(shared.DB_TABLENAME, test_db["schema"], port)
                (content, metadata, log) = runWorkload(port, seed, makeLog=True, logPoll=logPoll, logPipe=logPipe)
                closeReader(logPipe)
                if metadata["successful"]:
                    debug("seed", seed, "completed successfully in second try", level=1)
                    failed.remove(seed)
                    continue
                logAll(seed, id, metadata, log)
                
            if len(failed) == 0:
                debug("All tests succeeded on second try", level=1)
                
            cleanupEnvs()
    
    cleanupAll()
    debug("all done.", level=1)
