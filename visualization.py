import itertools
import json
from shared import SUT
from utils import traceHash, debug

def makeHTMLPage(metadata, log, containerId):
    debug("generating HTML report for id", containerId, level=2)
    
    # header, styling and metadata
    
    page = f"""<!DOCTYPE html><html><head><title>{containerId} | Transaction log</title><style>
table, td {{
    border: 1px solid lightgray;
    border-collapse: collapse;
}}
table {{
    white-space: nowrap;
}}
thead {{
    position: sticky;
    top: 0;
    z-index: 3;
}}
thead td {{
    border-bottom: 3px solid black;
    background: lightgray;
}}
td {{
    font-size: 16px;
    text-align: center;
}}
.tableInner {{
    width: 200px;
    min-height: 30px;
    padding: 0;
    position: relative;
    overflow: hidden;
}}
.openConnLine {{
    height: 100%;
    min-height: 30px;
    width: 3px;
    background: black;
    position: absolute;
    top: 0;
    bottom: 0;
    left: 50%;
}}
.commitLine {{
    height: 3px;
    width: 100%;
    background: green;
    position: absolute;
    top: 50%;
    left: 0;
    right: 0;
}}
.eventNumber {{
    padding: 0 10px;
    text-align: center;
}}
.farLeft {{
    position: sticky;
    left: 0;
    background: lightgray;
    border-right: 2px solid black;
    z-index: 2;
}}
.farRight {{
    position: sticky;
    right: 0;
    z-index: 2;
    background: lightgray;
    text-align: left;
    padding: 0 10px;
}}
details {{
    font-family: monospace;
}}
details > summary {{
    font-family: initial;
}}
.event {{
    border-radius: 10px;
    border: 2px solid black;
}}
.open {{
    border: 3px solid gray;
}}
.commit {{
    border: 3px solid green;
}}
.rollback {{
    border: 3px solid red;
}}
.failure {{
    background: lightcoral;
}}
#metadata {{
    padding-bottom: 30px;
}}
</style></head><body><div id="metadata">
<b>Transaction log for {containerId}</b><br/>
System under test (SUT): {SUT}<br/>
Seed: {str(metadata["seed"])} {"(given)" if metadata["seedGiven"] else "(generated)"}<br/>
Transactions: {metadata["transactions"]}<br/>
Concurrent connections: {metadata["concurrentConnections"]["avg"]} (avg) | {metadata["concurrentConnections"]["var"]} (var)<br/>
Transaction size: {metadata["transactionSize"]["avg"]} (avg) | {metadata["transactionSize"]["var"]} (var)<br/>
Statement size: {metadata["statementSize"]["avg"]} (avg) | {metadata["statementSize"]["var"]} (var)<br/>
Probability for Commit: {metadata["pCommit"]}<br/>
Probability for Rollback: {metadata["pRollback"]}<br/>
Probability for Insert: {metadata["pInsert"]}<br/>
Probability for Update: {metadata["pUpdate"]}<br/>
Probability for Delete: {metadata["pDelete"]}<br/>
Probability for Serialization Failure: {metadata["pSerializationFailure"]}<br/><br/>
Number of INSERTs: {metadata["numInsert"]}<br/>
Number of UPDATEs: {metadata["numUpdate"]} | of which produced concurrency conflict: {metadata["numCCUpdate"]} ({round(metadata["numCCUpdate"] / metadata["numUpdate"] * 100, 1)} %)<br/>
Number of DELETEs: {metadata["numDelete"]} | of which produced concurrency conflict: {metadata["numCCDelete"]} ({round(metadata["numCCDelete"] / metadata["numDelete"] * 100, 1)} %)<br/><br/>
Number of COMMITs: {metadata["numCommit"]} ({round(metadata["numCommit"] / metadata["transactions"] * 100, 1)} %)<br/>
Number of ROLLBACKs: {metadata["numRollback"]} ({round(metadata["numRollback"] / metadata["transactions"] * 100, 1)} %)<br/><br/>
Transaction trace was {"successful" if metadata["successful"] else "not successful"}<br/><br/>
<details><summary>Initial lazyfs log ({len(metadata["initialLog"])} line{"" if len(metadata["initialLog"]) == 1 else "s"})</summary>{"<br/>".join(metadata["initialLog"])}</details><br/>
Trace hash: {traceHash(log)}
</div><table><thead><tr><td></td>"""
    
    # table header
    
    for i in range(metadata["transactions"]):
        page += f"<td>{i}</td>"
    
    page += """<td class="farRight">lazyfs logs</td></tr></thead><tbody>"""
    
    # table rows
    
    openConns = []
    
    for event in itertools.batched(log, 2):
        if len(event) == 2:
            (info, _) = event
            if info["type"] == "open":
                openConns.append(info["transaction"])
            elif info["type"] == "commit" or info["type"] == "rollback":
                openConns.remove(info["transaction"])
        page += singleLine(event, metadata, openConns)
    
    # table footer, page end
    
    page += """</tbody></table></body></html>"""
    return page

def singleLine(batch, metadata, openConns):
    if len(batch) == 2:
        (event, status) = batch
    else:
        (event,) = batch
        status = {"result": "failure", "logs": []}
    
    line = "<tr>"
    
    # transaction number
    
    if event["type"] in ["insert", "update", "delete"]:
        line += f"""<td class="eventNumber farLeft">{event["statement"]}"""
    else:
        line += """<td class="farLeft"></td>"""
    
    # open transaction lines
    
    for i in range(event["transaction"]):
        line += """<td class="tableInner">"""
        if i in openConns:
            line += """<div class="openConnLine"></div>"""
        if event["type"] == "commit" and i >= min(openConns + [event["transaction"]]):
            line += """<div class="commitLine"></div>"""
        line += "</td>"
        
    # current statement
        
    if event["type"] == "open":
        line += f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} open">BEGIN<br/>Transaction {event["transaction"]}<br/>{event["numStatements"]} statements{"<br/>Failure" if status["result"] == "failure" else ""}</div></td>"""
    elif event["type"] == "insert":
        line += f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} insert">INSERT {event["count"]}{"<br/>Failure" if status["result"] == "failure" else ""}</div></td>"""
    elif event["type"] == "update":
        line += f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} update">UPDATE {event["count"]}{"<br/>Failure" if status["result"] == "failure" else ""}{"<br/>Serialization Failure,<br/>ROLLBACK" if status["result"] == "rollback" else ""}</div></td>"""
    elif event["type"] == "delete":
        line += f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} delete">DELETE {event["count"]}{"<br/>Failure" if status["result"] == "failure" else ""}{"<br/>Serialization Failure,<br/>ROLLBACK" if status["result"] == "rollback" else ""}</div></td>"""
    elif event["type"] == "commit":
        line += f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} commit">Transaction {event["transaction"]}<br/>COMMIT{"<br/>Failure" if status["result"] == "failure" else ""}</div></td>"""
    elif event["type"] == "rollback":
        line += f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} rollback">Transaction {event["transaction"]}<br/>ROLLBACK{"<br/>Failure" if status["result"] == "failure" else ""}</div></td>"""
        
    # open transaction lines
    
    for i in range(event["transaction"] + 1, metadata["transactions"]):
        line += """<td class="tableInner">"""
        if i in openConns:
            line += """<div class="openConnLine"></div>"""
        if event["type"] == "commit" and i >= min(openConns + [event["transaction"]]):
            line += """<div class="commitLine"></div>"""
        line += "</td>"
        
    # lazyfs log
    
    line += """<td class="farRight">"""
    
    if len(status["logs"]) > 0:
        line += f"""<details><summary>{len(status["logs"])} line{"" if len(status["logs"]) == 1 else "s"}</summary>"""
        line += "<br/>".join(status["logs"])
        line += "</details>"
    else:
        line += "0 lines"
    
    line += "</td>"
    
    return line

def makeTrace(log, containerId):
    debug("generating TRACE report for id", containerId, level=2)
    events = []
    
    openConns = 0
    ts = 0
    
    for b in itertools.batched(log, 2):
        if len(b) == 2:
            (e, s) = b
        else:
            (e,) = b
            s = {"result": "failure"}
            
        if e["type"] == "open":
            openConns += 1
            events.append({"name":"thread_name", "ph": "M", "tid": e["transaction"], "args": {"name": "Transaction"}})
            events.append({"name": "transaction", "ph": "B", "ts": ts, "tid": e["transaction"]})
            ts += 1
            events.append({"name": "Transactions", "ph": "C", "ts": ts, "args": {"open": openConns}})
            events.append({"name": "open", "ph": "X", "dur": 1, "tid": e["transaction"], "ts": ts})
        elif e["type"] == "insert":
            events.append({"name": "insert" + (" ROLLBACK" if s["result"] == "rollback" else ""), "ph": "X", "dur": 1, "tid": e["transaction"], "ts": ts})
        elif e["type"] == "update":
            events.append({"name": "update" + (" ROLLBACK" if s["result"] == "rollback" else ""), "ph": "X", "dur": 1, "tid": e["transaction"], "ts": ts})
        elif e["type"] == "delete":
            events.append({"name": "delete" + (" ROLLBACK" if s["result"] == "rollback" else ""), "ph": "X", "dur": 1, "tid": e["transaction"], "ts": ts})
        elif e["type"] == "commit":
            openConns -= 1
            events.append({"name": "commit", "ph": "X", "dur": 1, "tid": e["transaction"], "ts": ts})
            events.append({"name": "Transactions", "ph": "C", "ts": ts + 1, "args": {"open": openConns}})
            events.append({"name": "transaction", "ph": "E", "ts": ts + 1, "tid": e["transaction"]})
        elif e["type"] == "rollback":
            openConns -= 1
            events.append({"name": "rollback", "ph": "X", "dur": 1, "tid": e["transaction"], "ts": ts})
            events.append({"name": "Transactions", "ph": "C", "ts": ts + 1, "args": {"open": openConns}})
            events.append({"name": "transaction", "ph": "E", "ts": ts + 1, "tid": e["transaction"]})
        
        ts += 1
    
    return json.dumps({"traceEvents": events})