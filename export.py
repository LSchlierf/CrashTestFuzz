#!/usr/bin/env python
import json
import os
import shared
import sys
import traceback
import utils
import visualization

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} ./path/to/dump1.json ./path/to/dump2.json ...")
        exit(1)
    
    shared.DEBUG_LEVEL = 2
    for path in sys.argv[1:]:
        reexport(path)
    utils.info("done.")
    
def reexport(path):
    if not os.path.exists(path):
        utils.error(f"Invalid path: {path}")
        return
        
    if "logs/" in path:
        shared.SUT = path.split("logs/")[1].split("/")[0]
    else:
        sut = input(f"Enter SUT for {path}: ")
        if len(sut) > 0:
            shared.SUT = sut
        else:
            shared.SUT = "unknown"
    
    collectAndExport(path)

def collectAndExport(path):
    try:
        with open(path, "r") as f:
            data = [json.load(f)]
        
        if "parentID" in data[0] and data[0]["parentID"] != "": # test report
            
            depth = data[0]["testMetadata"]["depth"] if "testMetadata" in data[0] else data[0]["metadata"]["testMetadata"]["depth"]
            
            for _ in range(depth):
                parent = data[0]["parentID"]
                utils.debug("collecting parent", parent, level=1)
                parentFile = os.sep.join(path.split(os.sep)[:-1] + [parent + ".json"])
                if not os.path.exists(parentFile):
                    utils.error("Parent not found:", parent)
                    return
                with open(parentFile, "r") as f:
                    data.insert(0, json.load(f))            
            
        utils.dumpIntoFile(path.split(".json")[0] + "-wide.html", visualization.makeHTMLPage(data, path.split(".json")[0].split("/")[-1]), force=True) # wide
        utils.dumpIntoFile(path.split(".json")[0] + "-slim.html", visualization.makeHTMLPage(data, path.split(".json")[0].split("/")[-1], wide=False), force=True) # slim
        if "log" in data[-1]:
            utils.dumpIntoFile(path.split(".json")[0] + ".trace", visualization.makeTrace(data[-1]["log"], path.split(".json")[0].split("/")[-1]), force=True)
        
    except Exception:
        print(f"Error reading file: {traceback.format_exc()}")

if __name__ == "__main__":
    main()
