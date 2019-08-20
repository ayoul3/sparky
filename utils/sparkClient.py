from pyspark import SparkConf, SparkContext
from common import whine
import random
import logging
import subprocess
import os


class SparkClient:
    def __init__(self, target, port):
        self.target = target
        self.port = port
        self.logLevel = "FATAL"
        self.localIP = "192.168.1.22"
        self.sc = None
        self.appName = "ML test"
        self.version = ""

    def initContext(self):
        os.environ["SPARK_LOCAL_IP"] = self.localIP
        conf = SparkConf().setAppName(self.appName)
        conf = conf.setMaster("spark://%s:%d" % (self.target, self.port))
        conf = conf.set("spark.local.ip", self.localIP)
        conf = conf.set("spark.driver.host", self.localIP)
        self.sc = SparkContext(conf=conf)
        self.sc.addFile("/mnt/c/Users/ayoul3/Documents/spark/sparky/utils/cloud.py")
        import cloud

        self.sc.setLogLevel(self.logLevel)

    def isReady(self):
        if self.sc is None:
            return False
        return True

    def performWork(self):
        count = self.sc.parallelize(xrange(0, 1), 10).filter(lambda x: x + 1).count()

    def listNodes(self):
        if self.sc is None:
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
        if self.sc is None:
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

    def __del__(self):
        os.environ["SPARK_LOCAL_IP"] = ""
