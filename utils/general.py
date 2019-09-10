from prettytable import PrettyTable
import re
from .colors import Colors
import socket
import os, sys
import errno

toHex = lambda x: "".join([hex(ord(c))[2:].zfill(2) for c in x])


def whine(text, kind="clear", level=0):
    """
    Handles screen messages display
    """

    typeDisp, levelDisp, color = "", "", ""
    endColor = Colors.ENDC
    level = int(level)
    msgMap = {
        "warn": ["[!]", Colors.YELLOW],
        "info": ["[*]", Colors.BLUE],
        "err": ["[-]", Colors.RED],
        "good": ["[+]", Colors.GREEN],
        "clear": ["[ ]", ""],
    }

    typeDisp = msgMap.get(kind, ["[ ]", ""])[0]
    color = msgMap.get(kind, ["[ ]", ""])[1] if os.name != "nt" else ""
    endColor = Colors.ENDC if color != "" else ""

    levelDisp = "".join(["    "] * level)

    print(
        "{color}{level}{type}{endColor} {text}".format(
            color=color, level=levelDisp, type=typeDisp, text=text, endColor=endColor
        )
    )


def parseListNodes(sparkSession):
    whine(
        "Enumerating nodes and memory allocation on master %s:%d "
        % (sparkSession.target, sparkSession.port),
        "info",
    )

    listNodes = sparkSession.listNodes()
    if listNodes == None or len(listNodes) == 0:
        whine("Could not enumerate worker nodes", "err")
        return
    t = PrettyTable(
        ["Executor IP", "Ephemeral port", "Max caching mem.", "Free caching mem."]
    )

    for i in range(listNodes.size()):
        singleNode = listNodes.toList().apply(i)
        ip = str(singleNode._1()).split(":")[0]
        if ip == sparkSession.localIP:
            continue
        port = str(singleNode._1()).split(":")[1]
        maxMem = singleNode._2()._1()
        freeMem = singleNode._2()._2()
        t.add_row([ip, port, maxMem, freeMem])
    print(t)


def removeSpaces(input):
    input = input.replace("\n", ";").replace("\r", "")
    return str(re.sub("\s+", "${IFS}", input))
