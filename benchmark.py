import itertools
import json
import queue
import shared
import shutil
from threading import Thread
import traceback
from utils import *

test_db = {
    "name": shared.DB_TABLENAME,
    "schema": [("a", "int"), ("b", "int")]
}

parents = {}
results = {}
q = queue.Queue()

#########
# UTILS #
#########

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

def dumpTestMetadata(seed, id, testMetadata, parentID=""):
    if not os.path.exists(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}"):
        os.makedirs(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}", exist_ok=True)
    dumpIntoFile(
        f"logs/{shared.SUT}/{shared.TEST_RUN}/{seed}/{id}.json",
        json.dumps({"testMetadata": testMetadata, "parentID": parentID}, indent=4)
    )

def logAll(seed, id, metadata, log, restarts=0, parentID=""):
    if not os.path.exists(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}"):
        os.makedirs(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}", exist_ok=True)
    dumpIntoFile(
        f"logs/{shared.SUT}/{shared.TEST_RUN}/{seed}/{id}.json",
        json.dumps({"metadata": metadata, "log": log, "parentID": parentID}, indent=4)
    )
    try:
        copyLogs(seed, id, restarts)
        copyPersisted(seed, id)
    except:
        pass

##################
# SEED BENCHMARK #
##################

def runSeeds(makeLog, seeds):
    
    shared.TEST_RUN = "test-" + getFormattedTimestamp()

    buildSUTImage(wal_sync_method=shared.SYNC_METHOD)
    for (batch, seed) in enumerate(seeds):
        if os.path.exists(".terminate"):
            info("Terminating")
            os.remove(".terminate")
            break
        
        results.clear()
        parents.clear()
        
        debug("seed", seed, level=1)
        
        parentID = createAndPrepareContainer()
    
        debug("running workload without injected faults", level=1)

        duplicateID = duplicateContainer(parentID)
        port = runContainer(duplicateID)

        if not waitUntilAvailable(duplicateID, port, timeout=90):
            error("container didn't start, seed", seed)
            cleanupContainer(duplicateID)
            return

        (content, metadata, log) = runWorkload(port, duplicateID, seed, True)

        if not metadata["successful"]:
            error("first run failed, seed", seed)
            if makeLog in ["all", "failed"]:
                mergeLogs(metadata, log, duplicateID)
                logAll(seed, duplicateID, metadata, log)
            cleanupContainer(duplicateID)
            return

        cth = traceHash(log)
        debug("correct trace hash:", cth, level=1)

        stopSUT(duplicateID)

        lazyfsLogs = readLogs(duplicateID, "lazyfs")
        cleanupContainer(duplicateID)

        files = extractFiles(lazyfsLogs)
        if not os.path.exists(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}"):
            os.makedirs(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}", exist_ok=True)
        dumpIntoFile(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}/testfiles-{duplicateID}.json", json.dumps({"parent": parentID, "fileOps": files}, indent=4), force=True)

        maxOps = 0
        for item in files:
            if shared.FILE[0] in item and shared.OP[0] in files[item]:
                maxOps = max(maxOps, files[item][shared.OP[0]])

        stepSize = max(int(maxOps / shared.STEPS), 1)

        debug("max number of ops is", maxOps, "step size is", stepSize, level=3)

        hurdles = [(i+1) * stepSize for i in range(shared.STEPS)]

        debug(len(hurdles), "hurdles:", hurdles, level=3)
        
        for (index, hurdle) in enumerate(hurdles):
            q.put(Thread(target=runIteration, args=(duplicateID, parentID, [], batch, str(index), seed, hurdle, makeLog, shared.RECURSION_DEPTH, shared.STEPS)))
        
        threads = []
        
        # this is fine, this thread is the only consumer + any thread that enqueues new ones does so before terminating
        while not q.qsize() == 0:
            
            for _ in range(shared.CONCURRENT_TESTS):
                if q.qsize() == 0:
                    break
                threads.append(q.get())

            info("Starting group of", len(threads))
            
            for t in threads:
                t.start()
            
            for t in threads:
                t.join()
            
            threads.clear()
            
            info("Group finished,", q.qsize(), "queued")
        
        dumpIntoFile(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}/testResult.json", json.dumps({"parents": parents, "results": results}, indent=4), force=True)
        cleanupEnvs()

        info("Batch finished")
    
    cleanupAll()

def createAndPrepareContainer():
    id = prepHostEnvironment()
    port = runContainer(id)
    waitUntilAvailable(id, port, 90, kill=True)
    create(test_db["name"], test_db["schema"], port)
    stopContainer(id)
    return id

def runIteration(parentID, parentTemplateID, parentContent, batch, number, seed, hurdle, makeLog, remainingDepth, steps):
    
    tls.batch = batch
    tls.number = number
    
    depth = shared.RECURSION_DEPTH - remainingDepth
    file = shared.FILE[depth] if len(shared.FILE) > depth else shared.FILE[-1]
    timing = shared.TIMING[depth] if len(shared.TIMING) > depth else shared.TIMING[-1]
    operation = shared.OP[depth] if len(shared.OP) > depth else shared.OP[-1]
    
    debug("running workload with hurdle", hurdle, "on", file, timing, operation, level=1)
    
    cmd = "\n".join(["\n[[injection]]", "type = \"clear-cache\"", f"from = \"/tmp/lazyfs.root/{file}\"", f"timing = \"{timing}\"", f"op = \"{operation}\"", f"occurrence = {hurdle}", "crash = true"])
    
    childID = duplicateContainer(parentTemplateID)
    testMetadata = {
        "cmd": cmd,
        "hurdle": hurdle,
        "seed": seed,
        "parent": parentID,
        "depth": depth,
        "number": number,
        "checkpoint": shared.CHECKPOINT,
        "targetFile": file,
        "timing": timing,
        "operation": operation
    }
    
    port = runContainer(childID, crashcmd=cmd)
    startup = False
    if not waitUntilAvailable(childID, port, 90):
        info("no startup")
        testMetadata["result"] = "no-start"
        testMetadata["id"] = childID
        parents[childID] = parentID
        results[childID] = testMetadata
        stopContainer(childID, supressErrors=True)
        if makeLog in ["all", "failed"]:
            copyLogs(seed, childID, depth)
        content = parentContent
    
    else:
        (content, metadata, log) = runWorkload(port, childID, seed, True, dbContent=parentContent)
        mergeLogs(metadata, log, childID)
        startup = True
        testMetadata["traceHash"] = traceHash(log)
        stopContainer(childID, supressErrors=True)
        if metadata["successful"]:
            testMetadata["result"] = "initial-success"
            metadata["testMetadata"] = testMetadata
            if makeLog == "all":
                logAll(seed, childID, metadata, log, depth, parentID)
            parents[childID] = parentID
            results[childID] = testMetadata
            info("successful workflow, early return")
            return
        else:    
            if makeLog in ["all", "failed"]:
                copyLogs(seed, childID, depth)
    
    duplicateID = duplicateContainer(childID)
    port = runContainer(duplicateID)
    
    parents[duplicateID] = parentID
    testMetadata["template"] = childID
    testMetadata["id"] = duplicateID
    
    if startup:

        if not waitUntilAvailable(duplicateID, port, 90):
            info("container didn't restart")
            del metadata["oldSnapshots"]
            if "altContent" in metadata:
                del metadata["altContent"]
            testMetadata["result"] = "no-restart"
            metadata["testMetadata"] = testMetadata
            results[duplicateID] = testMetadata
            if makeLog in ["all", "failed"]:
                stopContainer(duplicateID, supressErrors=True)
                addRestartLog(metadata, duplicateID)
                logAll(seed, duplicateID, metadata, log, depth, parentID)
            cleanupContainer(duplicateID)
            cleanupContainer(childID)
            return

        if verify(shared.DB_TABLENAME, content, port, supressErrors=True):
            info("correct content", ("lost-commit" if "altContent" in metadata else ""))
            testMetadata["result"] = ("correct-content" + ("; lost-commit" if "altContent" in metadata else ""))
            del metadata["oldSnapshots"]
            if "altContent" in metadata:
                del metadata["altContent"]
            metadata["testMetadata"] = testMetadata
            results[duplicateID] = testMetadata
            stopContainer(duplicateID, supressErrors=True)
            if makeLog == "all":
                addRestartLog(metadata, duplicateID)
                logAll(seed, duplicateID, metadata, log, depth, parentID)

        elif "altContent" in metadata and verify(shared.DB_TABLENAME, metadata["altContent"], port, supressErrors=True):
            info("correct content, unconfirmed commit")
            testMetadata["result"] = "correct-content; unconfirmed-commit"
            content = metadata["altContent"]
            del metadata["oldSnapshots"]
            del metadata["altContent"]
            metadata["testMetadata"] = testMetadata
            results[duplicateID] = testMetadata
            stopContainer(duplicateID, supressErrors=True)
            if makeLog == "all":
                addRestartLog(metadata, duplicateID)
                logAll(seed, duplicateID, metadata, log, depth, parentID)  

        else:
            oldMatch = False
            for i in range(len(metadata["oldSnapshots"])):
                if verify(shared.DB_TABLENAME, metadata["oldSnapshots"][-(i + 1)], port, supressErrors=True):
                    oldMatch = True
                    lostCommits = i + 1
                    break
                
            if oldMatch:
                info(lostCommits, "lost commit(s)")
                testMetadata["result"] = f"incorrect-content; lost-commits: {lostCommits}"
                content = metadata["oldSnapshots"][-lostCommits]
                del metadata["oldSnapshots"]
                if "altContent" in metadata:
                    del metadata["altContent"]
                metadata["testMetadata"] = testMetadata
                results[duplicateID] = testMetadata
                stopContainer(duplicateID, supressErrors=True)
                if makeLog in ["all", "failed"]:
                    addRestartLog(metadata, duplicateID)
                    logAll(seed, duplicateID, metadata, log, depth, parentID)

            else:
                info("incorrect content")
                testMetadata["result"] = "incorrect-content"
                actual = dump(shared.DB_TABLENAME, port)
                mismatch = list(set(content) ^ set(actual))
                testMetadata["details"] = {"expected": content, "actual": actual, "mismatch": mismatch}
                del metadata["oldSnapshots"]
                if "altContent" in metadata:
                    del metadata["altContent"]
                metadata["testMetadata"] = testMetadata
                results[duplicateID] = testMetadata
                stopContainer(duplicateID, supressErrors=True)
                if makeLog in ["all", "failed"]:
                    addRestartLog(metadata, duplicateID)
                    logAll(seed, duplicateID, metadata, log, depth, parentID)
                cleanupContainer(duplicateID)
                cleanupContainer(childID)
                return
    
    else:
        
        if not waitUntilAvailable(duplicateID, port, 90):
            info("container didn't restart")
            testMetadata["result"] = "no-restart"
            results[duplicateID] = testMetadata
            stopContainer(duplicateID, supressErrors=True)
            if makeLog in ["all", "failed"]:
                copyLogs(seed, duplicateID, depth)
                addRestartLog(testMetadata, duplicateID)
                dumpTestMetadata(seed, duplicateID, testMetadata, parentID)
            cleanupContainer(duplicateID)
            cleanupContainer(childID)
            return
            
        if verify(shared.DB_TABLENAME, content, port, supressErrors=True):
            info("correct parent content")
            testMetadata["result"] = "correct-parent-content"
            results[duplicateID] = testMetadata
            stopContainer(duplicateID, supressErrors=True)
            if makeLog == "all":
                copyLogs(seed, duplicateID, depth)
                addRestartLog(testMetadata, duplicateID)
                dumpTestMetadata(seed, duplicateID, testMetadata, parentID)
            
        else:
            info("incorrect parent content")
            testMetadata["result"] = "incorrect-parent-content"
            actual = dump(shared.DB_TABLENAME, port)
            mismatch = list(set(content) ^ set(actual))
            testMetadata["details"] = {"expected": content, "actual": actual, "mismatch": mismatch}
            results[duplicateID] = testMetadata
            stopContainer(duplicateID, supressErrors=True)
            if makeLog in ["all", "failed"]:
                copyLogs(seed, duplicateID, depth)
                addRestartLog(testMetadata, duplicateID)
                dumpTestMetadata(seed, duplicateID, testMetadata, parentID)
            cleanupContainer(duplicateID)
            cleanupContainer(childID)
            return
    
    if remainingDepth == 0:
        info("recursion floor reached")
        cleanupContainer(childID)
        cleanupContainer(duplicateID)
        return
        
    lazyfsLogs = readLogs(duplicateID, "lazyfs")
    cleanupContainer(duplicateID)

    files = extractFiles(lazyfsLogs)
    if not os.path.exists(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}"):
        os.makedirs(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}", exist_ok=True)
    dumpIntoFile(f"logs/{shared.SUT}/{shared.TEST_RUN}/{str(seed)}/testfiles-{duplicateID}-{depth}.json", json.dumps({"parent": parentID, "fileOps": files}, indent=4), force=True)

    newFile = shared.FILE[depth + 1] if len(shared.FILE) > depth + 1 else shared.FILE[-1]
    newOp = shared.OP[depth + 1] if len(shared.OP) > depth + 1 else shared.OP[-1]

    maxOps = 0
    for item in files:
        if newFile in item and newOp in files[item]:
            maxOps = max(maxOps, files[item][newOp])

    stepSize = max(int(maxOps / steps), 1)

    debug("max number of ops is", maxOps, "step size is", stepSize, level=3)

    hurdles = [(i+1) * stepSize for i in range(steps)]

    debug(len(hurdles), "hurdles:", hurdles, level=3)
    
    for (index, h) in enumerate(hurdles):
        q.put(Thread(target=runIteration, args=(duplicateID, childID, content, batch, f"{number}.{index}", seed, h, makeLog, remainingDepth - 1, max(int(steps / shared.RECURSION_FACTOR), 1))))

#####################
# SEED VERIFICATION #
#####################

def verifySeedThreaded(batch, threadNumber, seed, results, makeLog):
    tls.number = f"{(threadNumber + 1):02d}"
    tls.batch = str(batch + 1)
    debug("verifying seed", seed, level=1)
    try:
        if verifySeed(seed, makeLog):
            results[threadNumber] = True
            debug("Success", level=1)
        else:
            error("Failure reported, seed", seed)
    except Exception as e:
        error(type(e), "exception occurred, seed", seed, traceback.format_exc())

def verifySeed(seed, makeLog):
    containerID = prepHostEnvironment()
    port = runContainer(containerID)
    waitUntilAvailable(containerID, port, timeout=90, kill=True)
    create(test_db["name"], schema=test_db["schema"], port=port)
    (content, metadata, log) = runWorkload(port, containerID, seed, makeLog=(makeLog in ["all", "failed"]), verification=True)
    if makeLog == "all" or (makeLog == "failed" and not metadata["successful"]):
        mergeLogs(metadata, log, containerID)
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
                port = runContainer(id)
                if not waitUntilAvailable(id, port, timeout=90):
                    error("container didn't start on retry, seed", seed)
                else:
                    create(shared.DB_TABLENAME, test_db["schema"], port)
                    (content, metadata, log) = runWorkload(port, id, seed, makeLog=True, verification=True)
                    if metadata["successful"]:
                        debug("seed", seed, "completed successfully in second try", level=1)
                        failed.remove(seed)
                        continue
                mergeLogs(metadata, log, id)
                logAll(seed, id, metadata, log)
                
            if len(failed) == 0:
                debug("All tests succeeded on second try", level=1)
                
            cleanupEnvs()
    
    cleanupAll()
    debug("all done.", level=1)
