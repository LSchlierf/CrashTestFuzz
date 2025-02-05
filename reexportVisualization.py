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
    
    if not os.path.exists(sys.argv[1]):
        print(f"Invalid path: {sys.argv[1]}")
        exit(1)
        
    shared.DEBUG_LEVEL = 2
    
    try:
        with open(sys.argv[1], "r") as f:
            data = json.load(f)
        
        utils.dumpIntoFile(sys.argv[1].split(".json")[0] + ".html", visualization.makeHTMLPage(data["metadata"], data["log"], sys.argv[1].split(".json")[0].split("/")[-1][2:]), force=True)
        utils.dumpIntoFile(sys.argv[1].split(".json")[0] + ".trace", visualization.makeTrace(data["log"], sys.argv[1].split(".json")[0].split("/")[-1][2:]), force=True)
        print("done.")
        
    except Exception as e:
        print(f"Error reading file: {e}")

if __name__ == "__main__":
    main()