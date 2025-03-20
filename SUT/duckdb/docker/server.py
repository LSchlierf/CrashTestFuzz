#!/usr/bin/env python3.10
import datetime
import duckdb
from fastapi import FastAPI
import uuid
import uvicorn

DB_DIR = "/var/db"

def getFormattedTimestamp():
    return datetime.datetime.now().strftime("[%Y-%m-%d@%H:%M:%S.%f]")

def log(*msg):
    print(getFormattedTimestamp(), *msg, flush=True)

log(f"Starting DuckDB (version: {duckdb.__version__})")

app = FastAPI()
conns = {}

@app.post("/open")
def openConn():
    connID = str(uuid.uuid4())
    log("opening new connection", connID)
    conns[connID] = duckdb.connect(f"{DB_DIR}/duck.db", read_only=False)
    conns[connID].begin()
    return {"connID": connID}

@app.post("/sql")
def sql(payload: dict):
    connID = payload.get("connID")
    if not connID in conns:
        log("error: invalid conn id", connID, "for sql()")
        return {"status": "error", "msg": "no such connection"}
    query = payload.get("query")
    if not query:
        log("error: no query")
        return {"status": "error", "msg": "no query"}
    log("running", query, "on conn", connID)
    try:
        conns[connID].execute(query) # never do this, kids!
        return {"status": "success"}
    except duckdb.TransactionException as e:
        log(str(e))
        return {"status": "concurrency conflict"}

@app.post("/fetchall")
def fetchall(payload: dict):
    connID = payload.get("connID")
    if not connID in conns:
        log("error: invalid conn id", connID, "for fetchall()")
        return {"status": "error", "msg": "no such connection"}
    log("fetchall on conn", connID)
    return {"status": "success", "result": conns[connID].fetchall()}

@app.post("/close")
def closeConn(payload: dict):
    connID = payload.get("connID")
    if not connID in conns:
        log("error: invalid conn id", connID, "for close()")
        return {"status": "error", "msg": "no such connection"}
    log("closing conn", connID)
    conns[connID].close()
    del conns[connID]
    return {"status": "success"}

@app.get("/ping")
def ping():
    return "pong"

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5432)