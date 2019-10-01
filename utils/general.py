from prettytable import PrettyTable
import re
from utils.colors import Colors
from lxml import html
import socket
import os, sys, re
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
    if listNodes == None:
        whine("Could not enumerate worker nodes", "err")
        return
    t = PrettyTable(
        [
            "Executor IP",
            "Ephemeral port",
            "Max caching mem. (B)",
            "Free caching mem. (B)",
        ]
    )
    totalWorkers = int(listNodes.size()) - 1
    whine("Total workers: %d" % totalWorkers, "good")
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


def confirmSpark(sClient):
    whine(
        "Testing target to confirm a spark master is running on {0}:{1}".format(
            sClient.target, sClient.port
        ),
        "info",
    )
    sparkConfirmed = sClient.sendHello()
    if not sparkConfirmed:
        whine("Could not confirm the target is running spark", "warn")
    elif sparkConfirmed and sClient.requiresAuthentication:
        whine(
            "Spark master confirmed at {0}:{1} - authentication required".format(
                sClient.target, sClient.port
            ),
            "warn",
        )
    else:
        whine(
            "Spark master confirmed at {0}:{1}".format(sClient.target, sClient.port),
            "good",
        )


def removeSpaces(input):
    input = input.replace("\n", ";").replace("\r", "")
    return str(re.sub("\s+", "${IFS}", input))


def checkRestPort(sClient):
    jsonData = sClient.sendRestHello()
    if jsonData is None:
        return
    sparkVersion = jsonData.get("serverSparkVersion", None)
    if not sparkVersion is None:
        whine("Spark version %s detected " % sparkVersion, "good")
    whine("Rest API available at %s:%s" % (sClient.target, sClient.restPort), "good")


def getTextFromNode(doc, xpathPattern, regex=None):
    raw_element = doc.xpath(xpathPattern)
    # print(raw_element)
    raw_element = (
        "".join("".join(raw_element).strip()) if len(raw_element) > 0 else None
    )
    if regex is None or raw_element is None:
        return raw_element
    raw_match = re.findall(regex, "".join(raw_element))
    if len(raw_match) > 0:
        return raw_match[0]
    return None


def getNumber(input):
    raw_match = re.findall("([\d,.]+ (?:\wB)?)", "".join(input))
    if len(raw_match) > 0:
        return raw_match[0]
    return 0


def getListWorks(doc):
    workerList = PrettyTable(
        ["Executor IP", "Executor port", "State", "Cores", "Memory"]
    )
    raw_list = doc.xpath('//div[contains(@class, "aggregated-workers")]/table//tr')
    if len(raw_list) < 1:
        return workerList
    for raw_tr in raw_list:
        try:
            host = raw_tr.xpath("./td[2]/text()")[0].split(":")[0]
            ip = raw_tr.xpath("./td[2]/text()")[0].split(":")[1]
            state = raw_tr.xpath("./td[3]/text()")[0]
            cores = raw_tr.xpath("./td[4]/text()")[0]
            memory = raw_tr.xpath("./td[5]/text()")[0]
            workerList.add_row([host, ip, state, getNumber(cores), getNumber(memory)])
        except:
            continue
    return workerList


def getListApps(doc):
    appList = PrettyTable(["App ID", "Name", "User", "State", "Duration"])
    activeApps = doc.xpath('//div[contains(@class, "aggregated-activeApps")]/table//tr')
    completedApps = doc.xpath(
        '//div[contains(@class, "aggregated-completedApps")]/table//tr'
    )
    allApps = activeApps + completedApps
    if len(allApps) < 1:
        return appList
    for raw_tr in allApps:
        try:
            appID = raw_tr.xpath("./td[1]/a/text()")[0]
            name = raw_tr.xpath("./td[2]/text()")[0]
            user = raw_tr.xpath("./td[6]/text()")[0]
            state = raw_tr.xpath("./td[7]/text()")[0]
            duration = raw_tr.xpath("./td[8]/text()")[0]
            appList.add_row([appID, name.strip(), user, state, duration])
        except:
            continue
    return appList


def extractClusterInfo(doc, version=None):
    clusterInfo = {}
    if version is None:
        clusterInfo["sparkVersion"] = getTextFromNode(
            doc, '//span[@class="version"]/text()'
        )
    clusterInfo["totalCores"] = getTextFromNode(
        doc, '//strong[text()="Cores in use:"]/parent::li/text()', "([\d,.]+)"
    )
    clusterInfo["totalMemory"] = getTextFromNode(
        doc, '//strong[text()="Memory in use:"]/parent::li/text()', "([\d,.]+ \wB) "
    )
    clusterInfo["totalWorkers"] = getTextFromNode(
        doc, '//strong[text()="Alive Workers:"]/parent::li/text()', "([\d,.]+)"
    )
    return clusterInfo


def checkHTTPPort(sClient):
    doc = sClient.sendHTTPHello()
    if doc is None:
        return False
    clusterInfo = extractClusterInfo(doc, sClient.version)
    listWorkers = getListWorks(doc)
    listApps = getListApps(doc)
    whine(
        "Master Web page status available at %s:%s"
        % (sClient.target, sClient.httpPort),
        "good",
    )
    for key, value in clusterInfo.items():
        whine("%s: %s" % (key, value), "info", 1)
    if listWorkers.rowcount > 0:
        whine("List of workers:", "info", 1)
        print(listWorkers.get_string())
    else:
        whine("No active workers detected", "warn", 1)
    if listApps.rowcount > 0:
        whine("List of apps:", "info", 1)
        print(listApps.get_string())

    return True
