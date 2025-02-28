#!/usr/bin/env python
import json
import os
import shared
import sys
import utils
import visualization

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} [./path/to/dump.json]")
        exit(1)
    
    shared.DEBUG_LEVEL = 2
    for path in sys.argv[1:]:
        export(path)
    utils.info("done.")
    
def export(path):
    if not os.path.exists(path):
        utils.error(f"Invalid path: {path}")
        return
        
    try:
        with open(path, "r") as f:
            data = json.load(f)
        
        utils.dumpIntoFile(path.split(".json")[0] + ".html", visualization.makeHTMLPage(data["metadata"], data["log"], path.split(".json")[0].split("/")[-1][2:]), force=True)
        utils.dumpIntoFile(path.split(".json")[0] + ".trace", visualization.makeTrace(data["log"], path.split(".json")[0].split("/")[-1][2:]), force=True)
        
    except Exception as e:
        print(f"Error reading file: {e}")

if __name__ == "__main__":
    main()