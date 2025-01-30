import json
import shared
from threading import Thread
from utils import *
from visualization import makeHTMLPage, makeTrace

FILE = "umbra.db.wal"
OP = "write"
STEPS = 20

SYNC_METHODS = [
    "none", "open_dsync", "open_sync", "fdatasync", "fsync"
]

test_db = {
    "name": shared.DB_TABLENAME,
    "schema": [("a", "int"), ("b", "int")]
}
    
def copyLogs(seed, id, success):
    shutil.copy(os.path.abspath(f"SUT/{shared.SUT}/container/container-{id}/{shared.SUT}.log"), os.path.abspath(f"logs/{shared.SUT}/{seed}/{'s' if success else 'f'}-{id}-{shared.SUT}.log"))
    shutil.copy(os.path.abspath(f"SUT/{shared.SUT}/container/container-{id}/lazyfs.log"), os.path.abspath(f"logs/{shared.SUT}/{seed}/{'s' if success else 'f'}-{id}-lazyfs.log"))

def logAll(seed, id, metadata, log):
    if not os.path.exists("logs/" + shared.SUT + "/" + str(seed)):
        os.makedirs("logs/" + shared.SUT + "/" + str(seed), exist_ok=True)
    dumpIntoFile("logs/" + shared.SUT + "/" + str(seed) + "/" + ("s-" if metadata["successful"] else "f-") + id + ".json", json.dumps({"metadata": metadata, "log": log}))
    dumpIntoFile("logs/" + shared.SUT + "/" + str(seed) + "/" + ("s-" if metadata["successful"] else "f-") + id + ".html", makeHTMLPage(metadata, log, id))
    dumpIntoFile("logs/" + shared.SUT + "/" + str(seed) + "/" + ("s-" if metadata["successful"] else "f-") + id + ".trace", makeTrace(log, id))
    copyLogs(seed, id, metadata["successful"])

def runTestThreaded(batch, threadNumber, hurdle, seed, results, makeLog):
    tld.number = threadNumber + 1
    tld.batch = batch + 1
    debug("running test in thread", threadNumber + 1, "hurdle is", hurdle, level=2)
    try:
        if runTest(hurdle, seed, makeLog):
            results[batch * shared.CONCURRENT_TESTS + threadNumber] = True
        else:
            error("Test failed, seed", seed)
    except:
        error("Test failed, seed", seed)
    
def runTest(hurdle, seed, makeLog):
    cmd = "\n".join(["[[injection]]", "type=\"clear\"", f"from=\"{FILE}\"", "timing=\"before\"", f"op=\"{OP}\"", f"occurrence={hurdle}", "crash=true"])
    id = prepHostEnvironment()
    if makeLog:
        (logPoll, logPipe) = openReader(id, "lazyfs")
    else:
        (logPoll, logPipe) = (None, None)
    port = runContainer(id, crashcmd=cmd)
    waitUntilAvailable(id, port, 90)
    sleep(8)
    create(shared.DB_TABLENAME, test_db["schema"], port)
    (content, metadata, log) = runWorkload(port, seed, makeLog=makeLog, logPoll=logPoll, logPipe=logPipe)
    if metadata["successful"] and verify(shared.DB_TABLENAME, content, port):
        debug("Test ran without faults", level=2)
        if makeLog:
            logAll(seed, id, metadata, log)
            debug("trace hash:", traceHash(log), level=2)
    stopSUT(id)
    try:
        stopContainer(id)
        port = runContainer(id)
        waitUntilAvailable(id, port, 90)
        sleep(8)
        if verify(shared.DB_TABLENAME, content, port):
            debug("Correct db content after restart", level=2)
            if makeLog:
                logAll(seed, id, metadata, log)
            cleanupContainer(id)
            return True
        else:
            error("Incorrect db content after restart")
            if makeLog:
                metadata["successful"] = False
                logAll(seed, id, metadata, log)
            cleanupContainer(id)
            return False
    except:
        error("Container didn't restart")
        if makeLog:
            metadata["successful"] = False
            logAll(seed, id, metadata, log)
        cleanupContainer(id)
        return False
        
def runSeedsThreaded(makeLog, start=0, end=10_000):
    

    for seed in range(start, end):
        os.system("clear")
        
        debug("seed", seed, level=1)

        results = [False for _ in range(STEPS)]
        
        # (actions, seed) = generateWorkload(1000, i)

        debug("running workload without injected faults", level=1)

        buildSUTImage()

        containerID = prepHostEnvironment()
        
        port = runContainer(containerID)

        sleep(5)

        create(shared.DB_TABLENAME, test_db["schema"], port=port)

        runWorkload(port, seed)

        # commandIntoFifo(containerID, "lazyfs::unsynced-data-report")

        stopSUT(containerID)

        # umbraLogs = readLogs(containerID, "umbra")
        lazyfsLogs = readLogs(containerID, "lazyfs")

        # debug("Umbra:\n" + ''.join(umbraLogs))
        # debug("Lazyfs:\n" + ''.join(lazyfsLogs))

        stopContainer(containerID)

        cleanupEnv(containerID)

        files = extractFiles(lazyfsLogs)

        # debug("Files:\n" + json.dumps(files, indent=2), level=3)

        maxOps = 0
        for item in files:
            if FILE in item and OP in files[item]:
                maxOps = max(maxOps, files[item][OP])

        stepSize = int(maxOps / STEPS)

        debug("max number of ops is", maxOps, "step size is", stepSize, level=3)

        hurdles = [(i+1) * stepSize for i in range(STEPS)]

        debug(len(hurdles), "hurdles:", hurdles, level=3)

        for (index, batch) in enumerate(itertools.batched(hurdles, shared.CONCURRENT_TESTS)):
            debug("running test batch", index + 1, level=1)
            threads = [Thread(target=runTestThreaded, args=(index, number, hurdle, seed, results, makeLog == "all")) for (number, hurdle) in enumerate(batch)]
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
            # debug(results, level=3)
            error("Errors in tests", [n for (n, s) in enumerate(results) if not s], "with hurdles", [hurdles[i] for (i, s) in enumerate(results) if not s])
            if makeLog == "failed":
                for h in [hurdles[i] for (i, s) in enumerate(results) if not s]:
                    runTest(h, seed, True)
            debug("seed was", seed, level=1)

        debug("all done.", level=1)

def verifySeedThreaded(batch, threadNumber, seed, results, makeLog):
    tld.number = threadNumber + 1
    tld.batch = batch + 1
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
    sleep(3)
    create(test_db["name"], schema=test_db["schema"], port=port)
    (content, metadata, log) = runWorkload(port, seed, makeLog=(makeLog in ["all", "failed"]), logPoll=logPoll, logPipe=logPipe)
    if makeLog == "all" or (makeLog == "failed" and not metadata["successful"]):
        logAll(seed, containerID, metadata, log)
        debug("trace hash:", traceHash(log), level=2)
    cleanupContainer(containerID)
    debug("seed was", seed, level=1)
    return metadata["successful"]

def verifySeedsThreaded(makeLog, start=0, end=10_000):

    buildSUTImage()

    for (i, b) in enumerate(itertools.batched(range(start,end), shared.CONCURRENT_TESTS)):
        # os.system("clear")
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
                sleep(3)
                create(shared.DB_TABLENAME, test_db["schema"], port)
                (content, metadata, log) = runWorkload(port, seed, makeLog=True, logPoll=logPoll, logPipe=logPipe)
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

