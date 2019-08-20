from utils.common import whine
from prettytable import PrettyTable


def parseListNodes(sparkSession):
    whine(
        "Enumerating nodes and memory allocation on master %s:%d "
        % (sparkSession.target, sparkSession.port),
        "info",
    )
    listNodes = sparkSession.listNodes()
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
