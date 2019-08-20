from utils.common import whine
from prettytable import PrettyTable


def parseCommandOutput(sparkSession, interpreterArgs, numWorkers):
    maxNodes = int(sparkSession.listNodes().size()) - 1
    whine("Found a maximum of %d workers that can be allocated" % maxNodes, "info")
    numWorkers = min(numWorkers, maxNodes)
    whine("Executing command on %d workers" % numWorkers, "info")
    listOutput = sparkSession.executeCMD(interpreterArgs, numWorkers)
    for i, out in enumerate(listOutput):
        whine("Output of worker %d" % i, "good")
        print(out)
