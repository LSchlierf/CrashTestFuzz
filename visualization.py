import itertools
import json
import shared
from utils import debug, getFormattedTimestamp, traceHash
    
successfulText = "<span style=\"color: green\">successful</span>"
unsuccessfulText = "<span style=\"color: red\">not successful</span>"

######################
# HTML Visualization #
######################

def makeHTMLPage(data, containerId, wide=True):
    debug("generating", ("wide" if wide else "slim"), "HTML report for id", containerId, level=2)
    
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
    z-index: -1;
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
    max-width: 30vw;
}}
.slimLogs {{
    max-width: 80vw;
    font-family: monospace;
    overflow: scroll;
    text-align: left;
    padding: 20px 10px
}}
details {{
    font-family: monospace;
    overflow: scroll;
}}
details > summary {{
    font-family: initial;
}}
.event {{
    background: white;
    border-radius: 10px;
    border: 2px solid black;
}}
.open {{
    border: 3px solid blue;
}}
.open + .openConnLine {{
    height: 50%;
    top: 50%;
}}
.commit {{
    border: 3px solid green;
}}
.rollback {{
    border: 3px solid red;
}}
.commit + .openConnLine, .rollback + .openConnLine {{
    height: 50%;
    bottom: 50%;
}}
.failure {{
    width: 20em;
    background: lightcoral;
    overflow-wrap: anywhere;
    white-space: normal;
}}
</style></head><body><div id="metadata">
<b>Transaction log for {containerId}</b><br/>
Generated {getFormattedTimestamp()}<br/><br/>
<b>Workload information</b><br/>
System under test (SUT): {shared.SUT}<br/>"""

    if "metadata" in data[0]:
        page += f"""
Seed: {str(data[0]["metadata"]["seed"])} {"(given)" if data[0]["metadata"]["seedGiven"] else "(generated)"}<br/>
Transactions: {data[0]["metadata"]["transactions"]}<br/>
Concurrent connections: {data[0]["metadata"]["concurrentConnections"]["avg"]} (avg) | {data[0]["metadata"]["concurrentConnections"]["var"]} (var)<br/>
Transaction size: {data[0]["metadata"]["transactionSize"]["avg"]} (avg) | {data[0]["metadata"]["transactionSize"]["var"]} (var)<br/>
Statement size: {data[0]["metadata"]["statementSize"]["avg"]} (avg) | {data[0]["metadata"]["statementSize"]["var"]} (var)<br/>
Probability for Commit: {data[0]["metadata"]["pCommit"]}<br/>
Probability for Rollback: {data[0]["metadata"]["pRollback"]}<br/>
Probability for Insert: {data[0]["metadata"]["pInsert"]}<br/>
Probability for Update: {data[0]["metadata"]["pUpdate"]}<br/>
Probability for Delete: {data[0]["metadata"]["pDelete"]}<br/>
Probability for Serialization Failure: {data[0]["metadata"]["pSerializationFailure"]}</div>"""

    for item in data:
        page += singleItem(item, wide)
    
    # table footer, page end
    
    page += """</body></html>"""
    return page

def singleItem(data, wide):
    if "metadata" in data:
        log = data["log"]
        metadata = data["metadata"]
        return f"""<div><br/><br/>{testMetadata(metadata)}
<b>Trace information</b><br/>
Number of INSERTs: {metadata["numInsert"]}<br/>
Number of UPDATEs: {metadata["numUpdate"]} | of which produced concurrency conflict: {metadata["numCCUpdate"]} ({round(metadata["numCCUpdate"] / metadata["numUpdate"] * 100, 1) if metadata["numUpdate"] != 0 else "0"}%) (Target: {round(metadata["pSerializationFailure"] * 100, 1)}%)<br/>
Number of DELETEs: {metadata["numDelete"]} | of which produced concurrency conflict: {metadata["numCCDelete"]} ({round(metadata["numCCDelete"] / metadata["numDelete"] * 100, 1) if metadata["numDelete"] != 0 else "0"}%) (Target: {round(metadata["pSerializationFailure"] * 100, 1)}%)<br/>
Number of COMMITs: {metadata["numCommit"]} ({round(metadata["numCommit"] / metadata["transactions"] * 100, 1) if metadata["transactions"] != 0 else "0"}%) (Target: {round(metadata["pCommit"] * 100, 1)}%)<br/>
Number of ROLLBACKs: {metadata["numRollback"]} ({round(metadata["numRollback"] / metadata["transactions"] * 100, 1) if metadata["transactions"] != 0 else "0"}%) (Target: {round(metadata["pRollback"] * 100, 1)}%) (Not including concurrency conflicts)<br/>
Trace hash: <b>{traceHash(log)}</b><br/>
Transaction trace was {successfulText if metadata["successful"] else unsuccessfulText}<br/><br/>
<details><summary>Initial log ({len(metadata["initialLog"])} line{"" if len(metadata["initialLog"]) == 1 else "s"})</summary>{"<br/>".join(metadata["initialLog"])}</details>
{additionalLog(metadata)}<br/></div>{wideTable(metadata, log) if wide else slimTable(log)}<br/><br/>"""

    else:
        return f"""<br/><br/>{testMetadata(data)}{additionalLog(data["testMetadata"], "startupLog")}<br/>{additionalLog(data["testMetadata"])}<br/>"""

def wideTable(metadata, log):
    # table header
    
    table = "<table><thead><tr><td></td>"
    
    for i in range(metadata["transactions"]):
        table += f"<td>{i}</td>"

    table += """<td class="farRight">logs</td></tr></thead><tbody>"""

    # table rows

    openConns = []

    for event in itertools.batched(log, 2):
        if len(event) == 2:
            (info, _) = event
            if info["type"] == "open":
                openConns.append(info["transaction"])
            elif info["type"] == "commit" or info["type"] == "rollback":
                openConns.remove(info["transaction"])
        table += singleWideLine(event, metadata, openConns)
    
    table += "</tbody></table>"
    
    return table

def singleWideLine(batch, metadata, openConns):
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
    
    line += statementItem(event, status)
    
    # open transaction lines
    
    for i in range(event["transaction"] + 1, metadata["transactions"]):
        line += """<td class="tableInner">"""
        if i in openConns:
            line += """<div class="openConnLine"></div>"""
        if event["type"] == "commit" and i >= min(openConns + [event["transaction"]]):
            line += """<div class="commitLine"></div>"""
        line += "</td>"
    
    # logs
    
    line += """<td class="farRight">"""
    
    if len(status["logs"]) > 0:
        line += f"""<details><summary>{len(status["logs"])} line{"" if len(status["logs"]) == 1 else "s"}</summary>"""
        line += "<br/>".join(status["logs"])
        line += "</details>"
    else:
        line += "0 lines"
    
    line += "</td></tr>"
    
    return line

def slimTable(log):
    table = "<table><thead><tr><td></td><td>Transaction item</td><td>logs</td></tr></thead>"
    
    for event in itertools.batched(log, 2):
        table += singleSlimLine(event)
    
    table += "</tbody></table>"
    
    return table

def singleSlimLine(batch):
    if len(batch) == 2:
        (event, status) = batch
    else:
        (event,) = batch
        status = {"result": "failure", "logs": []}
    
    line = "<tr>"
    
    if event["type"] in ["insert", "update", "delete"]:
        line += f"""<td class="eventNumber farLeft">{event["statement"]}"""
    else:
        line += """<td class="farLeft"></td>"""
    
    line += statementItem(event, status, nums=True)
    
    line += """<td><div class="slimLogs">"""
    
    if len(status["logs"]) > 0:
        line += "<br/>".join(status["logs"])
    else:
        line += "0 lines"
    
    line += "</div></td></tr>"
    
    line += "</tr>"
    return line

def statementItem(event, status, nums=False):
    
    if event["type"] == "open":
        return f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} open">Transaction {event["transaction"]}<br/>BEGIN<br/>{event["numStatements"]} statements{"<br/>Failure" if status["result"] == "failure" else ""}{":<br/>" + status["details"] if "details" in status else ""}</div>{"" if nums else "<div class='openConnLine'></div>"}</td>"""
    elif event["type"] == "insert":
        return f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} insert">{"Transaction " + str(event["transaction"]) + "<br/>" if nums else ""}INSERT {event["count"]}{"<br/>Failure" if status["result"] == "failure" else ""}{":<br/>" + status["details"] if "details" in status else ""}</div>{"" if nums else "<div class='openConnLine'></div>"}</td>"""
    elif event["type"] == "update":
        return f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} update">{"Transaction " + str(event["transaction"]) + "<br/>" if nums else ""}UPDATE {event["count"]}{"<br/>Failure" if status["result"] == "failure" else ""}{":<br/>" + status["details"] if "details" in status else ""}{"<br/>Serialization Failure,<br/>ROLLBACK" if status["result"] == "rollback" else ""}</div>{"" if nums else "<div class='openConnLine'></div>"}</td>"""
    elif event["type"] == "delete":
        return f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} delete">{"Transaction " + str(event["transaction"]) + "<br/>" if nums else ""}DELETE {event["count"]}{"<br/>Failure" if status["result"] == "failure" else ""}{":<br/>" + status["details"] if "details" in status else ""}{"<br/>Serialization Failure,<br/>ROLLBACK" if status["result"] == "rollback" else ""}</div>{"" if nums else "<div class='openConnLine'></div>"}</td>"""
    elif event["type"] == "commit":
        return f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} commit">Transaction {event["transaction"]}<br/>COMMIT{"<br/>Failure" if status["result"] == "failure" else ""}{":<br/>" + status["details"] if "details" in status else ""}</div>{"" if nums else "<div class='openConnLine'></div>"}</td>"""
    elif event["type"] == "rollback":
        return f"""<td class="tableInner"><div class="event{" failure" if status["result"] != "success" else ""} rollback">Transaction {event["transaction"]}<br/>ROLLBACK{"<br/>Failure" if status["result"] == "failure" else ""}{":<br/>" + status["details"] if "details" in status else ""}</div>{"" if nums else "<div class='openConnLine'></div>"}</td>"""
    
    return """<td class="tableInner>UNKNOWN EVENT</td>"""

def testMetadata(metadata):
    if not "testMetadata" in metadata:
        return ""
    
    d = metadata["testMetadata"]
    
    result = d["result"]
    
    if d["result"].startswith("correct-content"):
        result = f"""<span style="color: green">correct content after restart{d["result"][15:]}</span>"""
    elif d["result"] == "correct-parent-content":
        result = """<span style="color: green">correct (unchanged) content after crash during restart</span>"""
    elif d["result"].startswith("incorrect-content; lost"):
        result = f"""<span style="color: red">incorrect content after restart{d["result"][17:]}</span>"""
    elif d["result"].startswith("incorrect"):
        result = """<span style="color: red">incorrect content</span>"""
        if "details" in d:
            result += f"""<br/>{misMatchTable(d["details"])}"""
    elif d["result"] == "no-restart":
        result = """<span style="color: orange">container didn't restart</span>"""
    elif d["result"] == "error":
        result = """<span style="color: orange">error</span>"""
        if "details" in d:
            result += f"""<br>Details: {d["details"]}"""
    
    return f"""<b>Test information for depth {d["depth"]}</b><br/>
Number: {d["number"]}<br/>
Target file: {d["targetFile"]}<br/>
Timing: {d["timing"]}<br/>
Operation: {d["operation"]}<br/>
Occurrence: {d["hurdle"]}<br/>
Result: {result}<br/><br/>"""

def misMatchTable(details):
    expected = set(tuple(d) for d in details["expected"])
    actual = set(tuple(d) for d in details["actual"])
    
    mismatch = f"""<details><summary>Mismatch</summary>
Expected: {len(expected)} total<br/>Actual: {len(actual)} total<table>
<thead><tr><td>In simulated db</td><td>in actual db</td></tr></thead>"""

    complete = [(b,a) for (a,b) in sorted((b,a) for (a,b) in expected | actual)]
    
    expected = [(i if i in expected else "") for i in complete]
    actual = [(i if i in actual else "") for i in complete]

    for (e, a) in itertools.zip_longest(expected, actual, fillvalue=""):
        mismatch += f"""<tr><td>{e}</td><td>{a}</td></tr>"""
        
    mismatch += "</table></details>"
    
    return mismatch

def additionalLog(metadata, key="restartLog"):
    if not key in metadata or len(metadata[key]) == 0:
        return ""
    
    return f"""<details><summary>{key} ({len(metadata[key])} line{"" if len(metadata[key]) == 1 else "s"})</summary>{"<br/>".join(metadata[key])}</details>"""

#######################
# TRACE Visualization #
#######################

def makeTrace(log, containerId):
    # format docs: https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/edit?pli=1&tab=t.0#heading=h.yr4qxyxotyw
    # view with https://ui.perfetto.dev -> "open trace file"
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
