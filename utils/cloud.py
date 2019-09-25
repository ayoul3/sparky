import os.path, sys
import json
import pprint

try:
    import http.client as httplib
except:
    import httplib

try:
    from Queue import Queue
except:
    from queue import Queue

finalElements = Queue()
cloudInfo = {
    "aws": {
        "metadata": "/latest/meta-data/",
        "userdata": "/latest/user-data/",
        "privatekey": "/latest/meta-data/identity-credentials/",
    },
    "digitalocean": {
        "metadata": "/metadata/v1/",
        "userdata": "/metadata/v1/user-data",
        "privatekey": "",
    },
}


def fetchPath(path):
    #connection = httplib.HTTPConnection("192.168.1.25", 8080, timeout=2)
    connection = httplib.HTTPConnection("169.254.169.254", 80, timeout=2)
    connection.request("GET", path)
    response = connection.getresponse()
    if response.status == 200:
        s = response.read().strip()
        return s.decode("utf-8")
    connection.close()
    return ""


def parseData(path, input):
    if input[0] == "{":
        print(input)
        return
    for el in input.split("\n"):
        if el[len(el) - 1] == "/":
            finalElements.put("%s%s" % (path, el))
        else:
            output = fetchPath("%s%s" % (path, el))
            print("%s%s : %s " % (path, el, output))


def extractMetadata(firstURL):
    finalElements.put(firstURL)
    while True:
        try:
            path = finalElements.get(timeout=2)
        except:
            break
        output = fetchPath(path)
        if not output is None and len(output) > 0:
            parseData(path, output)


def getMetadata(metadataPath):
    extractMetadata(metadataPath)


def getUserdata(userdataPath):
    output = fetchPath(userdataPath)
    print(output)


def getPrivateKey(privatekeyPath):
    extractMetadata(privatekeyPath)


def whichCloud():
    if os.path.isfile("/etc/cloud/digitalocean.info"):
        return "digitalocean"
    elif "i-" in fetchPath("/latest/meta-data/instance-id"):
        return "aws"
    else:
        return "aws"


if __name__ == "__main__":
    try:
        arg = sys.argv[1]
    except:
        arg = "metadata"

    cloudProvider = whichCloud()
    if arg == "userdata":
        getUserdata(cloudInfo[cloudProvider]["userdata"])
    elif arg == "privatekey":
        getPrivateKey(cloudInfo["aws"]["privatekey"])
    else:
        getMetadata(cloudInfo[cloudProvider]["metadata"])
