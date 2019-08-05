from pyspark import SparkConf, SparkContext
import os

class SparkSession:

    def __init__(self, target, port):
        self.target = target
        self.port = port
        self.logLevel ="INFO"
        self.localIP = "192.168.1.22"
        self.sc = None
        self.appName = "Crunching numbers"


    def initContext(self):
        os.environ['SPARK_LOCAL_IP'] = '192.168.1.22'
        conf = SparkConf().setAppName(self.appName)
        conf = conf.setMaster("spark://%s:%d" % (self.target, self.port))
        conf = conf.set("spark.local.ip", self.localIP)
        conf = conf.set("spark.driver.host", self.localIP)
        self.sc = SparkContext(conf=conf)
        self.sc.setLogLevel(self.logLevel)

    def getInfo(self):
        if self.sc is None:
            return {}
        return self.sc._jsc.sc().listJars()

    def __del__(self):
        os.environ['SPARK_LOCAL_IP'] = ''
