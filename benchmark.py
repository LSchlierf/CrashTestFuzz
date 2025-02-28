import itertools
import json
import shared
import shutil
from threading import Thread
import traceback
from utils import *

test_db = {
    "name": shared.DB_TABLENAME,
    "schema": [("a", "int"), ("b", "int")]
}

def getTestMetadata():
    return {
        "targetFile": shared.FILE,
        "timing": shared.TIMING,
        "operation": shared.OP,
        "checkpoint": shared.CHECKPOINT
    }

def copyLogs(seed, id, restarts=0):
    shutil.copy(
        os.path.abspath(f"SUT/{shared.SUT}/container/container-{id}/{shared.SUT}.log"), 
        os.path.abspath(f"logs/{shared.SUT}/{shared.TEST_RUN}/{seed}/{id}-{shared.SUT}-{restarts}.log")
    )
    shutil.copy(
        os.path.abspath(f"SUT/{shared.SUT}/container/container-{id}/lazyfs.log"), 
        os.path.abspath(f"logs/{shared.SUT}/{shared.TEST_RUN}/{seed}/{id}-lazyfs-{restarts}.log")
    )

def copyPersisted(seed, id):
    shutil.copytree(
        os.path.abspath(f"SUT/{shared.SUT}/container/container-{id}/persisted"),
        os.path.abspath(f"logs/{shared.SUT}/{shared.TEST_RUN}/{seed}/{id}-persisted"),
        dirs_exist_ok=True
    )

def logAll(seed, id, metadata, log, restarts=0):
    if not os.path.exists(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}"):
        os.makedirs(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}", exist_ok=True)
    dumpIntoFile(
        f"logs/{shared.SUT}/{shared.TEST_RUN}/{seed}/{id}.json",
        json.dumps({"metadata": metadata, "log": log}, indent=4)
    )
    try:
        copyLogs(seed, id, restarts)
        copyPersisted(seed, id)
    except:
        pass

def runTestThreaded(batch, threadNumber, hurdle, seed, results, ids, makeLog):
    tls.number = threadNumber + 1
    tls.batch = batch + 1
    debug("running test in thread", threadNumber + 1, "hurdle is", hurdle, level=2)
    try:
        (result, id) = runTest(hurdle, seed, makeLog)
        ids[batch * shared.CONCURRENT_TESTS + threadNumber] = id
        if result == False:
            error("Test failed, seed", seed)
        else:
            results[batch * shared.CONCURRENT_TESTS + threadNumber] = result
    except Exception as e:
        error("Test failed with exception", type(e), e, traceback.format_exc(), "seed", seed)

def runTest(hurdle, seed, makeLog):
    cmd = "\n".join(["\n[[injection]]", "type = \"clear-cache\"", f"from = \"/tmp/lazyfs.root/{shared.FILE}\"", f"timing = \"{shared.TIMING}\"", f"op = \"{shared.OP}\"", f"occurrence = {hurdle}", "crash = true"])
    id = prepHostEnvironment()
    testMetadata = {
        "cmd": cmd,
        "hurdle": hurdle,
        "seed": seed,
        **getTestMetadata()
    }
    (logPoll, logPipe) = openReader(id, "lazyfs")
    port = runContainer(id, crashcmd=cmd)
    if not waitUntilAvailable(id, port, 90):
        return (False, id)
    try:
        create(shared.DB_TABLENAME, test_db["schema"], port)
    except:
        return (False, id)
    (content, metadata, log) = runWorkload(port, id, seed, True, logPoll=logPoll, logPipe=logPipe)
    closeReader(logPipe)
    initialSuccess = False
    if metadata["successful"] and verify(shared.DB_TABLENAME, content, port):
        initialSuccess = True
        info("Initial trace ran without faults")
        stopSUT(id)
    else:
        info("Initial trace ran with faults")
    testMetadata["traceHash"] = traceHash(log)
    debug("trace hash:", testMetadata["traceHash"], level=1)
    try:
        stopContainer(id, supressErrors=True)
        if makeLog in ["all", "failed"]:
            copyLogs(seed, id, 0)
        port = runContainer(id)
        if not waitUntilAvailable(id, port, 600):
            info("Container didn't restart")
            testMetadata["result"] = "no-restart"
            metadata["testMetadata"] = testMetadata
            if makeLog in ["all", "failed"]:
                stopContainer(id, supressErrors=True)
                logAll(seed, id, metadata, log, 1)
            cleanupContainer(id)
            return ({"result": testMetadata["result"], "traceHash": testMetadata["traceHash"], "initialTraceSuccessful": initialSuccess}, id)
        if verify(shared.DB_TABLENAME, content, port, supressErrors=True):
            if "altContent" in metadata:
                debug("Correct db content after restart, lost commit", level=1)
            else:
                debug("Correct db content after restart", level=1)
            testMetadata["result"] = ("correct-content" + ("; lost-commit" if "altContent" in metadata else ""))
            metadata["testMetadata"] = testMetadata
            if makeLog == "all":
                stopContainer(id, supressErrors=True)
                logAll(seed, id, metadata, log, 1)
            cleanupContainer(id)
            return ({"result": testMetadata["result"], "traceHash": testMetadata["traceHash"], "initialTraceSuccessful": initialSuccess}, id)
        else:
            if "altContent" in metadata and verify(shared.DB_TABLENAME, metadata["altContent"], port, supressErrors=True):
                debug("Correct db content after restart, unconfirmed commit", level=1)
                testMetadata["result"] = "correct-content; unconfirmed-commit"
                metadata["testMetadata"] = testMetadata
                if makeLog == "all":
                    stopContainer(id, supressErrors=True)
                    logAll(seed, id, metadata, log, 1)
                cleanupContainer(id)
                return ({"result": testMetadata["result"], "traceHash": testMetadata["traceHash"], "initialTraceSuccessful": initialSuccess}, id)
            info("Incorrect db content after restart")
            testMetadata["result"] = "incorrect-content"
            metadata["testMetadata"] = testMetadata
            if makeLog in ["all", "failed"]:
                stopContainer(id, supressErrors=True)
                logAll(seed, id, metadata, log, 1)
            cleanupContainer(id)
            return ({"result": testMetadata["result"], "traceHash": testMetadata["traceHash"], "initialTraceSuccessful": initialSuccess}, id)
    except Exception as e:
        error(type(e), "exception occured:", e, traceback.format_exc())
        testMetadata["result"] = "error"
        metadata["testMetadata"] = testMetadata
        if makeLog in ["all", "failed"]:
            stopContainer(id, supressErrors=True)
            logAll(seed, id, metadata, log, 1)
        cleanupContainer(id)
        return ({"result": testMetadata["result"], "traceHash": testMetadata["traceHash"], "initialTraceSuccessful": initialSuccess}, id)

def runSeedsThreaded(makeLog, seeds):
    
    shared.TEST_RUN = "test-" + getTimeStamp()

    buildSUTImage(wal_sync_method=shared.SYNC_METHOD)
    for seed in seeds:
        if os.path.exists(".terminate"):
            info("Terminating")
            os.remove(".terminate")
            break
        debug("seed", seed, level=1)

        results = [{} for _ in range(shared.STEPS)]
        ids = ["" for _ in range(shared.STEPS)]

        debug("running workload without injected faults", level=1)

        containerID = prepHostEnvironment()
        port = runContainer(containerID)

        if not waitUntilAvailable(containerID, port, timeout=90):
            error("container didn't start, seed", seed)
            if log in ["all", "failed"]:
                logAll(seed, containerID, metadata, log)
            cleanupContainer(containerID)
            continue
        
        create(shared.DB_TABLENAME, test_db["schema"], port=port)
        
        (poll, pipe) = openReader(containerID, shared.SUT)

        (content, metadata, log) = runWorkload(port, containerID, seed, True, poll, pipe)
        closeReader(pipe)
        if not metadata["successful"]:
            error("first run failed, seed", seed)
            if log in ["all", "failed"]:
                logAll(seed, containerID, metadata, log)
            cleanupContainer(containerID)
            continue

        cth = traceHash(log)
        debug("correct trace hash:", cth, level=1)
        
        stopSUT(containerID)

        lazyfsLogs = readLogs(containerID, "lazyfs")
        cleanupContainer(containerID)

        files = extractFiles(lazyfsLogs)
        if not os.path.exists(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}"):
            os.makedirs(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}", exist_ok=True)
        dumpIntoFile(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}/testfiles.json", json.dumps(files, indent=4), force=True)

        maxOps = 0
        for item in files:
            if shared.FILE in item and shared.OP in files[item]:
                maxOps = max(maxOps, files[item][shared.OP])

        stepSize = max(int(maxOps / shared.STEPS), 1)

        debug("max number of ops is", maxOps, "step size is", stepSize, level=3)

        hurdles = [(i+1) * stepSize for i in range(shared.STEPS)]

        debug(len(hurdles), "hurdles:", hurdles, level=3)
        
        for (index, batch) in enumerate(itertools.batched(hurdles, shared.CONCURRENT_TESTS)):
            
            debug("running test batch", index + 1, level=1)
            threads = [Thread(target=runTestThreaded, args=(index, number, hurdle, seed, results, ids, makeLog)) for (number, hurdle) in enumerate(batch)]
            
            for (n, t) in enumerate(threads):
                debug("starting thread", n + 1, level=2)
                t.start()
            for (n, t) in enumerate(threads):
                debug("joining thread", n + 1, level=2)
                t.join()

            debug("finished test batch", index + 1, level=1)
            cleanupEnvs(supressErrors=True)

        if not False in results:
            debug("all tests ran without errors.", level=1)
        else:
            error("Errors in tests", [n for (n, s) in enumerate(results) if not s], "with hurdles", [hurdles[i] for (i, s) in enumerate(results) if not s])
            if makeLog == "retry":
                for h in [hurdles[i] for (i, s) in enumerate(results) if not s]:
                    runTest(h, seed, "all")
                cleanupEnvs(supressErrors=True)
            debug("seed was", seed, level=1)
        
        testResults = [{"hurlde": hurdles[i], "result": results[i], "id": ids[i]} for i in range(shared.STEPS)]
        dumpIntoFile(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}/testMetadata.json", json.dumps({"parameters": {
            "SUT": shared.SUT,
            "generated": getTimeStamp(),
            "seed": seed,
            "correctTraceHash": cth,
            "sync_method": shared.SYNC_METHOD,
            "steps": shared.STEPS,
            **getTestMetadata(),
            **getMetadata()
        }, "results": testResults}, indent=4), force=True)

    cleanupAll(supressErrors=True)
    debug("all done.", level=1)

#####################
# SEED VERIFICATION #
#####################

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
    waitUntilAvailable(containerID, port, timeout=90, kill=True)
    create(test_db["name"], schema=test_db["schema"], port=port)
    (content, metadata, log) = runWorkload(port, containerID, seed, makeLog=(makeLog in ["all", "failed"]), logPoll=logPoll, logPipe=logPipe, verification=True)
    closeReader(logPipe)
    if makeLog == "all" or (makeLog == "failed" and not metadata["successful"]):
        logAll(seed, containerID, metadata, log)
        info("trace hash:", traceHash(log))
    cleanupContainer(containerID)
    debug("seed was", seed, level=1)
    return metadata["successful"]

def verifySeedsThreaded(makeLog, seeds):

    buildSUTImage()

    for (i, b) in enumerate(itertools.batched(seeds, shared.CONCURRENT_TESTS)):
        if os.path.exists(".terminate"):
            info("Terminating")
            os.remove(".terminate")
            break
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
            error("Errors in tests", [n for (n, s) in enumerate(results) if not s], "with seeds", [b[i] for (i, s) in enumerate(results) if not s])
            failed = [b[i] for (i, s) in enumerate(results) if not s]
            for seed in failed.copy():
                id = prepHostEnvironment()
                (logPoll, logPipe) = openReader(id, "lazyfs")
                port = runContainer(id)
                if not waitUntilAvailable(id, port, timeout=90):
                    error("container didn't start on retry, seed", seed)
                else:
                    create(shared.DB_TABLENAME, test_db["schema"], port)
                    (content, metadata, log) = runWorkload(port, id, seed, makeLog=True, logPoll=logPoll, logPipe=logPipe, verification=True)
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
