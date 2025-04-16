import datetime
from hashlib import md5
import itertools
import json
import os
import psycopg2
import random
import requests
import shared
import shutil
import subprocess
import sys
from threading import local
import time
import traceback
import uuid

####################
# FORMATTING UTILS #
####################

def getThreadId():
    if hasattr(tls, "number"):
        return f" \033[34m[Thread {tls.batch}|{tls.number}]\033[0m"
    return ""

def getFormattedTimestamp():
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def getTimestamp():
    return datetime.datetime.now().timestamp()

def error(*msg, kill=False):
    print("\033[31m[ ERROR ]\033[0m \033[32m" + getFormattedTimestamp() + "\033[0m" + getThreadId(), *msg, flush=True)
    if(kill):
        exit(1)

def debug(*msg, multiline=False, level=3):
    if level <= shared.DEBUG_LEVEL:
        print("\033[33m[DEBUG-" + str(level) + "]\033[0m \033[32m" + getFormattedTimestamp() + "\033[0m" + getThreadId() + ("\n" if multiline else ""), *msg, flush=True)

def info(*msg):
    print("\033[36m[ INFO  ]\033[0m \033[32m" + getFormattedTimestamp() + "\033[0m" + getThreadId(), *msg, flush=True)

##################
# WORKLOAD UTILS #
##################

def runWorkload(port, id, seed=None, makeLog=False, verification=False, dbContent=[]):
    """runs a workload on a given postgres port
    
    Arguments:
    port                        (required) - port the db is listening on
    seed                        (optional) - seed for the RNG. if missing, on will be generated from the system time.
    makeLog                     (optional) - if set to True, all opens, statements, rollbacks and commits will be logged and returned (default: False)
    verification                (optional) - if set to True, will verify db content after each commit and save snapshots of db (default: False)
    
    All other parameters are passed via the shared module
    """
    # TODO: document following shared inputs
    """
    Given via shared module:
    transactions                (required) - number of transactions
    concurrentConnections       (optional) - mean and std dev for how many connections should be concurrently open (default: (3.0, 1.0))
    transactionSize             (optional) - mean and std dev for how many data items should be inserted / updated / deleted (default: 100.0, 5.0)
    statementsPerTransaction    (optional) - mean and std dev for how many statements should be run per transaction (default: (5.0, 1.0))
    pCommitted                  (optional) - probability that transactions end in commit (1 - p for rollback) (default: 0.7)
    pInsert                     (optional) - probability that statements will be insert (default: 0.7)
    pUpdate                     (optional) - probability that statements will be update (default: 0.2)
                                            -> probability for delete will be 1 - pInsert - pUpdate (default: 0.1)
    """
    debug("running workload on port", port, level=2)
    seedMissing = seed is None
    if seedMissing:
        seed = int(time.time()) % 100_000
        debug("no seed given", level=3)
    remainingTransactions = shared.NUM_TRANSACTIONS
    debug("seed:", seed, "number of transactions:", remainingTransactions)
    r = random.Random(seed)
    openConns = []
    finishedTransactions = []
    lockedItems = set()
    log = []
    (ccMu, ccVar) = shared.CONCURRENT_TRANSACTIONS
    (sMu, sVar) = shared.STATEMENT_SIZE
    (tMu, tVar) = shared.TRANSACTION_SIZE
    metadata = {
        "seed": seed,
        "seedGiven": not seedMissing,
        "successful": False,
        "numInsert": 0,
        "numUpdate": 0,
        "numDelete": 0,
        "numCommit": 0,
        "numRollback": 0,
        "numCCUpdate": 0,
        "numCCDelete": 0,
        **getMetadata(),
        "oldSnapshots": [],
        "initialLog": []
    }
    cid = 0
    aid = 0
    while len(finishedTransactions) > 0 or len(openConns) > 0 or remainingTransactions > 0:
        
        ccs = r.gauss(ccMu, ccVar)
        
        if (len(openConns) == 0 and len(finishedTransactions) == 0 or ccs > (len(openConns) + len(finishedTransactions)) and hash(ccs) % 2 == 0) and remainingTransactions > 0:
        
            #########################
            # BEGIN NEW TRANSACTION #
            #########################
            
            numStatements = max(round(r.gauss(tMu, tVar)), 1) # at least 1 stmt per transaction
            
            debug("New connection", cid, ",", numStatements, "statements, starting point is", dbContent, level=4)
            
            if makeLog:
                log.append({
                    "type": "open",
                    "timestamp": getTimestamp(),
                    "transaction": cid,
                    "numStatements": numStatements
                })
            
            try:
                newConn = connect(port)
            except Exception as e:
                if verification:
                    error(type(e), "exception occurred during open", e)
                if makeLog:
                    log.append({"result": "failure", "logs": [], "details": str(e).strip()})
                metadata["result"] = "error"
                metadata["details"] = str(e).strip()
                return (dbContent, metadata, log)
            
            openConns.append({"c": newConn, "id": cid, "numStatements": numStatements, "statements": [], "localContent": dbContent.copy(), "lockedVals": set()})
            
            remainingTransactions -= 1
            cid += 1
        
        elif len(finishedTransactions) == 0:
            
            ####################################
            # EXECUTE STATEMENT ON TRANSACTION #
            ####################################
            
            transactionIndex = r.randrange(len(openConns))
            currConn = openConns[transactionIndex]
            stmtTypeP = r.random()
            count = max(1, round(r.gauss(sMu, sVar)))
            
            expectCC = False
            
            if stmtTypeP < shared.P_INSERT or len(set(currConn["localContent"]) - (lockedItems - currConn["lockedVals"])) < count:
                
                ##########
                # INSERT #
                ##########
                
                stmtType = "insert"
                metadata["numInsert"] += 1
                values = [(len(currConn["localContent"]) + i, aid) for i in range(count)]
                
                debug("insert", count, "on transaction", currConn["id"], values, level=4)
                
                if makeLog:
                    log.append({
                        "type": "insert",
                        "timestamp": getTimestamp(),
                        "transaction": currConn["id"],
                        "statement": aid,
                        "count": count,
                        "values": values.copy()
                    })
                
                fun = clientInsert
                args = (values.copy())
                
                try:
                    insert(currConn["c"], values)
                    clientInsert((currConn["localContent"], values))
                except Exception as e:
                    if verification:
                        error(type(e), "exception occurred during insert", e)
                    if makeLog:
                        log.append({"result": "failure", "logs": [], "details": str(e).strip()})
                    metadata["result"] = "error"
                    metadata["details"] = str(e).strip()
                    return (dbContent, metadata, log)
                
            elif stmtTypeP < shared.P_INSERT + shared.P_UPDATE:
                
                ##########
                # UPDATE #
                ##########
                
                stmtType = "update"
                metadata["numUpdate"] += 1
                expectCC = r.random() < shared.P_SERIALIZATION_FAILURE and len(set(currConn["localContent"]) & (lockedItems - currConn["lockedVals"])) >= count
                
                debug(len(set(currConn["localContent"]) & (lockedItems - currConn["lockedVals"])), level=4)
                
                if expectCC:
                    valsToEdit = [v for v in currConn["localContent"] if v in (lockedItems - currConn["lockedVals"])][-count:]
                    metadata["numCCUpdate"] += 1
                    debug("expecting serialization failure", level=4)
                else:
                    valsToEdit = [v for v in currConn["localContent"] if not v in (lockedItems - currConn["lockedVals"])][-count:]
                
                
                debug("update", count, "on transaction", currConn["id"], aid, valsToEdit, level=4)
                
                if makeLog:
                    log.append({
                        "type": "update",
                        "timestamp": getTimestamp(),
                        "transaction": currConn["id"],
                        "statement": aid,
                        "count": count,
                        "values": valsToEdit.copy()
                    })
                
                fun = clientUpdate
                args = (valsToEdit.copy(), aid)
                
                try:
                    
                    update(currConn["c"], valsToEdit, aid)
                    if expectCC:
                        if verification:
                            error("Expected concurrency conflict")
                        if makeLog:
                            log.append({"result": "failure", "logs": [], "details": "expected concurrency conflict"})
                        metadata["result"] = "expected-concurrency-conflict"
                        return (dbContent, metadata, log)
                    clientUpdate((currConn["localContent"], (valsToEdit, aid)))
                    currConn["lockedVals"] |= set(valsToEdit)
                    lockedItems |= set(valsToEdit)
                
                except (psycopg2.errors.SerializationFailure, psycopg2.errors.LockNotAvailable):
                    
                    if not expectCC:
                        error("Didn't expect concurrency conflict")
                        if makeLog:
                            log.append({"result": "failure", "logs": [], "details": "didn't expect concurrency conflict"})
                        metadata["result"] = "didnt-expect-concurrency-conflict"
                        return (dbContent, metadata, log)
                    debug("concurrency conflict, need to rollback", level=4)
                    currConn["c"].rollback()
                    with currConn["c"].cursor() as c:
                        c.execute(f"SELECT 0;") # used as BEGIN;
                    currConn["statements"] = []
                    currConn["localContent"] = dbContent.copy()
                    lockedItems -= currConn["lockedVals"]
                    currConn["lockedVals"] = set()
                    aid += 1
                    
                    if makeLog:
                        log.append({"result": "rollback", "logs": []})
                    
                    continue
                
                except Exception as e:
                    if verification:
                        error(type(e), "exception occurred during update,", ("cc" if expectCC else "no cc"), e)
                    if makeLog:
                        log.append({"result": "failure", "logs": [], "details": str(e).strip()})
                    metadata["result"] = "error"
                    metadata["details"] = str(e).strip()
                    return (dbContent, metadata, log)
                
            else:
                
                ##########
                # DELETE #
                ##########
                
                stmtType = "delete"
                metadata["numDelete"] += 1
                expectCC = r.random() < shared.P_SERIALIZATION_FAILURE and len(set(currConn["localContent"]) & (lockedItems - currConn["lockedVals"])) >= count
                
                debug(len(set(currConn["localContent"]) & (lockedItems - currConn["lockedVals"])), level=4)
                
                if expectCC:
                    valsToRm = [v for v in currConn["localContent"] if v in (lockedItems - currConn["lockedVals"])][-count:]
                    metadata["numCCDelete"] += 1
                    debug("expecting serialization failure", level=4)
                else:
                    valsToRm = [v for v in currConn["localContent"] if not v in (lockedItems - currConn["lockedVals"])][-count:]
                
                debug("delete", count, "on transaction", currConn["id"], valsToRm, level=4)
                
                if makeLog:
                    log.append({
                        "type": "delete",
                        "timestamp": getTimestamp(),
                        "transaction": currConn["id"],
                        "statement": aid,
                        "count": count,
                        "values": valsToRm.copy()
                    })
                
                fun = clientDelete
                args = (valsToRm.copy())
                
                try:
                    
                    delete(currConn["c"], valsToRm)
                    if expectCC:
                        if verification:
                            error("Expected concurrency conflict")
                        if makeLog:
                            log.append({"result": "failure", "logs": [], "details": "expected concurrency conflict"})
                        metadata["result"] = "expected-concurrency-conflict"
                        return (dbContent, metadata, log)
                    clientDelete((currConn["localContent"], valsToRm))
                    currConn["lockedVals"] |= set(valsToRm)
                    lockedItems |= set(valsToRm)
                
                except (psycopg2.errors.SerializationFailure, psycopg2.errors.LockNotAvailable):
                    
                    if not expectCC:
                        error("Didn't expect concurrency conflict")
                        if makeLog:
                            log.append({"result": "failure", "logs": [], "details": "didn't expect concurrency conflict"})
                        metadata["result"] = "didnt-expect-concurrency-conflict"
                        return (dbContent, metadata, log)
                    debug("concurrency conflict, need to rollback", level=4)
                    currConn["c"].rollback()
                    with currConn["c"].cursor() as c:
                        c.execute(f"SELECT 0;") # used as BEGIN;
                    currConn["statements"] = []
                    currConn["localContent"] = dbContent.copy()
                    lockedItems -= currConn["lockedVals"]
                    currConn["lockedVals"] = set()
                    aid += 1
                    
                    if makeLog:
                        log.append({"result": "rollback", "logs": []})
                    
                    continue
                
                except Exception as e:
                    if verification:
                        error(type(e), "exception occurred during delete,", ("cc" if expectCC else "no cc"), e)
                    if makeLog:
                        log.append({"result": "failure", "logs": [], "details": str(e).strip()})
                    metadata["result"] = "error"
                    metadata["details"] = str(e).strip()
                    return (dbContent, metadata, log)
            
            aid = aid + 1
            currConn["statements"].append((fun, args))
            
            debug(stmtType, count, "in transaction", currConn["id"], level=4)
            
            if len(currConn["statements"]) >= currConn["numStatements"]:
                debug("all statements run on conn", currConn["id"], level=4)
                finishedTransactions.append(openConns.pop(transactionIndex))
        
        else:

            ######################
            # FINISH TRANSACTION #
            ######################
            
            transaction = finishedTransactions.pop(r.randrange(len(finishedTransactions)))
            commit = r.random() < shared.P_COMMIT
            
            if commit:
                
                ######################
                # COMMIT TRANSACTION #
                ######################

                debug("commit transaction", transaction["id"], level=4)
                metadata["numCommit"] += 1
                
                if makeLog:
                    log.append({
                        "type": "commit",
                        "timestamp": getTimestamp(),
                        "transaction": transaction["id"]
                    })
                    
                newContent = dbContent.copy()
                for (f, args) in transaction["statements"]:
                    f((newContent, args))
                
                try:
                    transaction["c"].commit()
                    transaction["c"].close()
                except Exception as e:
                    if verification:
                        error(type(e), "exception occurred during commit", e)
                    metadata["altContent"] = newContent
                    if makeLog:
                        log.append({"result": "failure", "logs": [], "details": str(e).strip()})
                    metadata["result"] = "error"
                    metadata["details"] = str(e).strip()
                    return (dbContent, metadata, log)
                if not verification:
                    metadata["oldSnapshots"].append(dbContent)
                dbContent = newContent
            
            else:
                
                ########################
                # ROLLBACK TRANSACTION #
                ########################

                debug("rollback transaction", transaction["id"], level=4)
                metadata["numRollback"] += 1
                
                lockedItems -= transaction["lockedVals"]

                if makeLog:
                    log.append({
                        "type": "rollback",
                        "timestamp": getTimestamp(),
                        "transaction": transaction["id"]
                    })
                
                try:
                    transaction["c"].rollback()
                    transaction["c"].close()
                except Exception as e:
                    if verification:
                        error(type(e), "exception occurred during rollback", e)
                    if makeLog:
                        log.append({"result": "failure", "logs": [], "details": str(e).strip()})
                    metadata["result"] = "error"
                    metadata["details"] = str(e).strip()
                    return (dbContent, metadata, log)
            
            # debug(dbContent, level=4)
            try:
                if verification and not verify(shared.DB_TABLENAME, dbContent, port):
                    if makeLog:
                        log.append({"result": "failure", "logs": [], "details": "verify mismatch"})
                        metadata["result"] = "verify mismatch"
                        metadata["details"] = {"expected": dbContent, "actual": dump(shared.DB_TABLENAME, port)}
                    return (dbContent, metadata, log)
                if shared.CHECKPOINT:
                    commandIntoFifo(id, "lazyfs::cache-checkpoint")
            except Exception as e:
                if makeLog:
                    log.append({"result": "failure", "logs": [], "details": str(e).strip()})
                metadata["result"] = "error"
                metadata["details"] = str(e).strip()
                return (dbContent, metadata, log)
        
        #########################
        # LOG SUCCESSFUL ACTION #
        #########################
        
        if makeLog:
            log.append({"result": "success", "logs": []})

    ###############################
    # ALL TRANSACTIONS SUCCESSFUL #
    ###############################

    metadata["successful"] = True
    return (dbContent, metadata, log)

def getMetadata():
    (ccMu, ccVar) = shared.CONCURRENT_TRANSACTIONS
    (sMu, sVar) = shared.STATEMENT_SIZE
    (tMu, tVar) = shared.TRANSACTION_SIZE
    return {
        "transactions": shared.NUM_TRANSACTIONS,
        "concurrentConnections": { "avg": ccMu, "var": ccVar },
        "transactionSize": { "avg": tMu, "var": tVar },
        "statementSize": { "avg": sMu, "var": sVar },
        "pCommit": shared.P_COMMIT,
        "pRollback": round(1 - shared.P_COMMIT, 5),
        "pInsert": shared.P_INSERT,
        "pUpdate": shared.P_UPDATE,
        "pDelete": round(1 - shared.P_INSERT - shared.P_UPDATE, 5),
        "pSerializationFailure": shared.P_SERIALIZATION_FAILURE
    }

def SUTTimestamp(line):
    if shared.SUT == "postgres":
        if len(line) < 24 or line[4] != '-' or line[7] != '-' or line[10] != ' ' or line[13] != ':' or line[16] != ':' or line[19] != '.':
            return 0
        (year, month, day, hour, minute, second, millisecond) = (int(line[0:4]), int(line[5:7]), int(line[8:10]), int(line[11:13]), int(line[14:16]), int(line[17:19]), int(line[20:23]))
        return datetime.datetime(year, month, day, hour, minute, second, millisecond * 1000, datetime.UTC).timestamp()
    
    if shared.SUT == "cedardb":
        if len(line) < 24 or line[4] != '-' or line[7] != '-' or line[10] != ' ' or line[13] != ':' or line[16] != ':' or line[19] != '.':
            return 0
        (year, month, day, hour, minute, second, microsecond) = (int(line[0:4]), int(line[5:7]), int(line[8:10]), int(line[11:13]), int(line[14:16]), int(line[17:19]), int(line[20:26]))                
        return datetime.datetime(year, month, day, hour, minute, second, microsecond, datetime.UTC).timestamp()
    
    if shared.SUT in ["duckdb", "sqlite"]:
        if len(line) < 24 or line[0] != '[' or line[5] != '-' or line[8] != '-' or line[11] != '@' or line[14] != ':' or line[17] != ':' or line[20] != '.':
            return 0
        (year, month, day, hour, minute, second, microsecond) = (int(line[1:5]), int(line[6:8]), int(line[9:11]), int(line[12:14]), int(line[15:17]), int(line[18:20]), int(line[21:27]))
        return datetime.datetime(year, month, day, hour, minute, second, microsecond, datetime.UTC).timestamp()
    
    return 0

def lazyfsTimestamp(line):
    if len(line) < 24:
        return 0
    (year, month, day, hour, minute, second, millisecond) = (int(line[1:5]), int(line[6:8]), int(line[9:11]), int(line[12:14]), int(line[15:17]), int(line[18:20]), int(line[21:24]))
    return datetime.datetime(year, month, day, hour, minute, second, millisecond * 1000, datetime.UTC).timestamp()

def mergeLogs(metadata, log, containerID):
    lazyfslogs = [line for line in readLogs(containerID, "lazyfs") if not "lfs_getattr(" in line]
    sutlogs = readLogs(containerID, shared.SUT)
    
    if (len(log) % 2) == 1:
        log.append({"result": "failure", "logs": []})
    
    targets = [metadata["initialLog"]] + [b[1]["logs"] for b in itertools.batched(log, 2)]
    timestamps = [b[0]["timestamp"] for b in itertools.batched(log, 2)] + [sys.float_info.max]
    
    for (target, timestamp) in zip(targets, timestamps, strict=True):
        
        while (len(sutlogs) > 0 and SUTTimestamp(sutlogs[0]) < timestamp) or (len(lazyfslogs) > 0 and lazyfsTimestamp(lazyfslogs[0]) < timestamp):
            if len(sutlogs) == 0 or (len(lazyfslogs) > 0 and lazyfsTimestamp(lazyfslogs[0]) < SUTTimestamp(sutlogs[0])):
                target.append(f"[lazyfs] {lazyfslogs.pop(0)}")
            else:
                target.append(f"[{shared.SUT}] {sutlogs.pop(0)}")

def addLog(metadata, containerID, dest="restartLog"):
    lazyfslogs = [line for line in readLogs(containerID, "lazyfs") if not "lfs_getattr(" in line]
    sutlogs = readLogs(containerID, shared.SUT)
    
    if not dest in metadata:
        metadata[dest] = []
    
    while len(lazyfslogs) > 0 or len(sutlogs) > 0:
        if len(sutlogs) == 0 or (len(lazyfslogs) > 0 and lazyfsTimestamp(lazyfslogs[0]) < SUTTimestamp(sutlogs[0])):
            metadata[dest].append(f"[lazyfs] {lazyfslogs.pop(0)}")
        else:
            metadata[dest].append(f"[{shared.SUT}] {sutlogs.pop(0)}")

#####################
# SQL CONTROL UTILS #
#####################

class apiConnection:
    def __init__(self, port):
        self._port = port
        self._connID = requests.post(f"http://127.0.0.1:{self._port}/open").json()["connID"]

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def cursor(self):
        return apiCursor(self)

    def commit(self):
        result = requests.post(f"http://127.0.0.1:{self._port}/sql", json={"connID": self._connID, "query": "COMMIT;"}).json()["status"]
        assert result == "success"

    def rollback(self):
        requests.post(f"http://127.0.0.1:{self._port}/sql", json={"connID": self._connID, "query": "ROLLBACK;"})
        self.close()
        self._connID = requests.post(f"http://127.0.0.1:{self._port}/open").json()["connID"]

    def close(self):
        result = requests.post(f"http://127.0.0.1:{self._port}/close", json={"connID": self._connID}).json()["status"]
        assert result == "success"

class apiCursor:
    def __init__(self, connection):
        self._conn = connection
        self.rowcount = 0
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        pass

    def execute(self, stmt):
        result = requests.post(f"http://127.0.0.1:{self._conn._port}/sql", json={"connID": self._conn._connID, "query": stmt}).json()["status"]
        if "concurrency conflict" in result:
            raise psycopg2.errors.SerializationFailure()
        assert result == "success"

    def fetchall(self):
        return [tuple(v) for v in requests.post(f"http://127.0.0.1:{self._conn._port}/fetchall", json={"connID": self._conn._connID}).json()["result"]]

def connect(port):
    if shared.SUT in ["duckdb", "sqlite"]:
        conn = apiConnection(port)
    elif shared.SUT == "postgres":
        conn = psycopg2.connect(user="postgres", host="localhost", port=port)
        conn.set_session(isolation_level="REPEATABLE READ")
        with conn.cursor() as c:
            c.execute(f"SELECT 0;") # used as BEGIN;
    else:
        conn = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="localhost", port=port)
        with conn.cursor() as c:
            c.execute(f"SELECT 0;") # used as BEGIN;
    return conn

def create(name, schema, port):
    debug("creating db", level=2)
    with connect(port) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS " + name + ";")
        cur.execute("CREATE TABLE " + name + " (" + ", ".join([s[0] + " " + s[1] for s in schema]) + ");")
        conn.commit()
    debug("\033[1mdone\033[0m creating db")

def insert(conn, values):
    cur = conn.cursor()
    cur.execute("INSERT INTO " + shared.DB_TABLENAME + " VALUES " + ', '.join(str(v) for v in values) + ";")
    debug(cur.rowcount, level=4)

def clientInsert(args):
    (content, values) = args
    content[len(content):] = values

def update(conn, vals, newAction):
    cur = conn.cursor()
    if shared.SUT == "postgres":
        cur.execute("SELECT * FROM " + shared.DB_TABLENAME + " WHERE " + " or ".join(["(a = " + str(a) + " and b = " + str(b) + ")" for (a, b) in vals]) + " FOR UPDATE NOWAIT;")
        cur.execute("UPDATE " + shared.DB_TABLENAME + " SET b = " + str(newAction) + " WHERE " + " or ".join(["(a = " + str(a) + " and b = " + str(b) + ")" for (a, b) in vals]) + ";")
    else:
        cur.execute("UPDATE " + shared.DB_TABLENAME + " SET b = " + str(newAction) + " WHERE " + " or ".join(["(a = " + str(a) + " and b = " + str(b) + ")" for (a, b) in vals]) + ";")
    debug(cur.rowcount, level=4)

def clientUpdate(args):
    (content, vals) = args
    (valsToEdit, newAction) = vals
    for i in range(len(content)):
        (a, b) = content[i]
        if (a, b) in valsToEdit:
            content[i] = (a, newAction)

def delete(conn, vals):
    cur = conn.cursor()
    if shared.SUT == "postgres":
        cur.execute("SELECT * FROM " + shared.DB_TABLENAME + " WHERE " + " or ".join(["(a = " + str(a) + " and b = " + str(b) + ")" for (a, b) in vals]) + " FOR UPDATE NOWAIT;")
        cur.execute("DELETE FROM " + shared.DB_TABLENAME + " WHERE " + " or ".join(["(a = " + str(a) + " and b = " + str(b) + ")" for (a, b) in vals]) + ";")
    else:
        cur.execute("DELETE FROM " + shared.DB_TABLENAME + " WHERE " + " or ".join(["(a = " + str(a) + " and b = " + str(b) + ")" for (a, b) in vals]) + ";")
    debug(cur.rowcount, level=4)

def clientDelete(args):
    (content, valsToRm) = args
    for v in valsToRm:
        while v in content:
            content.remove(v)

def dump(name, port):
    with connect(port) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + name + ";")
        return cur.fetchall()

def verify(name, content, port, kill=False, supressErrors=False):
    debug("verifying", name, level=2)
    try:
        data = dump(name, port)
    except Exception as e:
        if not supressErrors:
            error(type(e), "exception occurrec during dump:", traceback.format_exc())
        return False
    content = set(str(c) for c in content)
    data = set(str(c) for c in data)
    cross = data ^ content
    if len(content) != len(data):
        if not supressErrors:
            error("verify: length mismatch:", len(content), "(local) vs", len(data), "(db)", kill=False)
            error("db has", str(data - content), kill=False)
            error("local has", str(content - data), kill=kill)
        return False
    if len(cross) != 0:
        if not supressErrors:
            error("verify: mismatch:", str(cross), kill=False)
            error("db has", str(data - content), kill=False)
            error("local has", str(content - data), kill=kill)
        return False
    debug("\033[1mdone\033[0m verifying", level=2)
    return True

###############################
# SUT CONTAINER CONTROL UTILS #
###############################

def buildSUTImage(wal_sync_method=None):
    if os.path.exists(f"/dev/shm/ctf/{shared.SUT}"):
        error(f"Another instance of CrashTestFuzz testing {shared.SUT} is running or a previous run failed.\nIn the latter case, clean up all orphaned docker containers and remove /dev/shm/ctf/{shared.SUT} as well as ./SUT/{shared.SUT}/container", kill=True)
    os.makedirs(f"/dev/shm/ctf/{shared.SUT}", exist_ok=True)
    os.symlink(f"/dev/shm/ctf/{shared.SUT}", os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "container"]))
    debug("building SUT, " + "no WAL_SYNC_METHOD given" if wal_sync_method is None else "WAL_SYNC_METHOD is " + wal_sync_method, level=2)
    r = subprocess.run(["bash", "./build-image.sh", ("" if wal_sync_method is None else wal_sync_method)], cwd=os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "scripts"]), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        error("building SUT failed with code", r.returncode)
        error(r.stdout.decode())
        error(r.stderr.decode(), kill=True)
    debug("\033[1mdone\033[0m building", level=2)

def prepHostEnvironment(containerID=None):
    debug("preparing host environment", level=2)
    if containerID is None:
        containerID = str(uuid.uuid4())
        debug("no container ID given, generated ID " + containerID, level=3)
    r = subprocess.run(["bash", "./prep-env.sh", containerID], cwd=os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "scripts"]), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        error("preparing host environment failed with code", r.returncode)
        error(r.stdout.decode())
        error(r.stderr.decode(), kill=True)
    debug("\033[1mdone\033[0m preparing env", level=2)
    return containerID

def duplicateContainer(containerID, newContainerID=None):
    debug("duplicating container", containerID, level=2)
    if newContainerID is None:
        newContainerID = str(uuid.uuid4())
        debug("no container ID given, generated ID " + newContainerID, level=3)
    r = subprocess.run(["bash", "./duplicate-container.sh", containerID, newContainerID], cwd=os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "scripts"]), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # TODO: duplicate logs from prev runs
    if r.returncode != 0:
        error("duplicating container failed with code", r.returncode)
        error(r.stdout.decode())
        error(r.stderr.decode(), kill=True)
    debug("\033[1mdone\033[0m duplicating container", level=2)
    return newContainerID

def runContainer(containerID, port=0, crashcmd=""):
    debug("running container", containerID, level=2)
    r = subprocess.run(["bash", "./run-container.sh", containerID, str(port), crashcmd], cwd=os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "scripts"]), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        error("running container failed with code", r.returncode)
        error(r.stdout.decode())
        error(r.stderr.decode(), kill=True)
    if port == 0:
        debug("No port given", level=3)
        port = getPort(containerID)
    debug("\033[1mdone\033[0m running", level=2)
    return port

def stopSUT(containerID, supressErrors=False):
    debug("stopping SUT", containerID, level=2)
    r = subprocess.run(["bash", "./stop-sut.sh", containerID], cwd=os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "scripts"]), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0 and not supressErrors:
        error("stopping SUT failed with code", r.returncode)
        error(r.stdout.decode())
        error(r.stderr.decode(), kill=True)
    debug("\033[1mdone\033[0m stopping SUT", level=2)

def stopContainer(containerID, supressErrors=False):
    debug("stopping container", containerID, level=2)
    r = subprocess.run(["bash", "./stop-container.sh", containerID], cwd=os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "scripts"]), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0 and not supressErrors:
        error("stopping container failed with code", r.returncode)
        error(r.stdout.decode())
        error(r.stderr.decode())
    debug("\033[1mdone\033[0m stopping container", level=2)

def cleanupEnv(containerID):
    debug("cleaning up host environment", containerID, level=2)
    r = subprocess.run(["bash", "./cleanup-env.sh", containerID], cwd=os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "scripts"]), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        error("cleaning up host environment failed with code", r.returncode)
        error(r.stdout.decode())
        error(r.stderr.decode())
    debug("\033[1mdone\033[0m cleaning up env", level=2)

def cleanupContainer(containerID):
    stopContainer(containerID, supressErrors=True)
    cleanupEnv(containerID)

def cleanupEnvs(supressErrors=False):
    debug("cleaning up all envs", level=2)
    r = subprocess.run(["bash", "./cleanup-envs.sh"], cwd=os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "scripts"]), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0 and not supressErrors:
        error("cleaning up all envs failed with code", r.returncode)
        error(r.stdout.decode())
        error(r.stderr.decode())
    debug("\033[1mdone\033[0m cleaning up", level=2)

def cleanupAll(supressErrors=False):
    debug("cleaning up all", level=2)
    r = subprocess.run(["bash", "./cleanup-all.sh"], cwd=os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "scripts"]), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0 and not supressErrors:
        error("cleaning up all failed with code", r.returncode)
        error(r.stdout.decode())
        error(r.stderr.decode())
    os.remove(os.sep.join([os.path.dirname(os.path.abspath(__file__)), "SUT", shared.SUT, "container"]))
    os.rmdir(f"/dev/shm/ctf/{shared.SUT}")
    debug("\033[1mdone\033[0m cleaning up", level=2)

def getPort(containerID):
    port = 0
    tries = 1
    while port == 0:
        result = subprocess.check_output("docker ps", shell=True).decode().strip()
        debug("Looking for container port", level=3)
        if tries >= 10:
            debug("this is attempt", tries, level=3)
        debug("Got output\n", result, level=4)
        if containerID in result:
            lines = result.splitlines()
            for l in lines:
                if containerID in l:
                    port = int(l.split("0.0.0.0:")[1].split("->5432")[0])
                    debug("got port", port, level=3)
                    return port
        else:
            if tries == 20:
                error("Container port for", containerID, "not found after 20 tries, likely didn't start.")
                exit(1)
            tries += 1
            time.sleep(0.5)

def waitUntilAvailable(id, port, timeout=0, kill=False, supressErrors=False):
    secs = 0
    while True:
        if shared.SUT == "postgres":
            logs = "\n".join(readLogs(id, "postgres")[-20:])
            if "database system is ready to accept connections" in logs:
                sleep(3)
                return True
        elif shared.SUT in ["duckdb", "sqlite"]:
            try:
                if requests.get(f"http://127.0.0.1:{port}/ping").text == '"pong"':
                    sleep(3)
                    return True
            except:
                pass
        else:
            try:
                c = connect(port)
                c.close()
                sleep(3)
                return True
            except:
                pass
        if timeout != 0 and secs >= timeout:
            if not supressErrors:
                error("Timeout while waiting for system start after", timeout, "seconds", kill=kill)
            return False
        secs += 1
        sleep(1)

######################
# FILE CONTROL UTILS #
######################

def commandIntoFifo(containerID, cmd):
    debug("writing into fifo for container", containerID, cmd, level=2)
    fifo = os.open(os.sep.join(["SUT", shared.SUT, "container", "container-" + containerID, "faults.fifo"]), os.O_WRONLY | os.O_NONBLOCK)
    os.write(fifo, (cmd + "\n").encode())
    os.close(fifo)
    debug("\033[1mdone\033[0m writing", level=2)

def readLogs(containerID, name):
    debug("reading logs", name, "of container", containerID, level=2)
    with open(os.sep.join(["SUT", shared.SUT, "container", "container-" + containerID, name + ".log"])) as log:
        logs = log.readlines()
    debug("\033[1mdone\033[0m reading logs", level=2)
    return logs

def dumpIntoFile(path, content, force=False):
    if force:
        with open(path, "w") as f:
            f.write(content)
    else:
        with open(path, "x") as f:
            f.write(content)

##############
# MISC UTILS #
##############

tls = local()

def sleep(secs):
    debug(f"sleeping for {secs} seconds", level=2)
    time.sleep(secs)

def traceHash(log):
    newLog = []
    # filter out logs, timestamps
    for item in log:
        if "result" in item:
            newLog.append({"result": item["result"]})
        elif "type" in item:
            newItem = {**item}
            del newItem["timestamp"]
            newLog.append(newItem)
    return md5(json.dumps(newLog).encode(encoding="utf-8")).hexdigest()

def extractFiles(logs):
    files = {}
    for line in logs:
        if not "[lazyfs.ops]" in line or not "lazyfs.root" in line:
            continue
        path = line.split("lazyfs.root/")[1].split(",")[0].split(")")[0]
        op = line.split("lfs_")[1].split("(path=")[0].split("(")[0]
        if not path in files:
            files[path] = {}
        if not op in files[path]:
            files[path][op] = 0
        files[path][op] += 1
    return files
