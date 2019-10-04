import os, sys, hashlib, re
import string, base64, random
import struct, socket
from utils.general import whine


def randomBytes(stringLength=10):
    letters = string.ascii_lowercase
    return "".join([random.choice(letters) for i in range(stringLength)]).encode()


def getSASLHash(nonce, realm, secret, username, digestURI="null/default"):
    b64Username = base64.b64encode(username.encode("utf-8"))
    b64Secret = base64.b64encode(secret.encode("utf-8"))
    cnonce = randomBytes(40)
    realmBytes = realm.encode()
    nonceBytes = nonce.encode("utf-8")

    FirstHalfA1_hash = hashlib.md5()
    FirstHalfA1_hash.update(b64Username + b":" + realmBytes + b":" + b64Secret)

    A1_hash = hashlib.md5()
    A1_hash.update(FirstHalfA1_hash.digest())
    A1_hash.update(b":" + nonceBytes + b":" + cnonce)
    HHA1 = A1_hash.hexdigest()

    A2_hash = hashlib.md5()
    A2_hash.update(b"AUTHENTICATE:")
    A2_hash.update(digestURI.encode())

    HHA2 = A2_hash.hexdigest()
    resp_hash = hashlib.md5()

    message = (
        HHA1.encode()
        + b":"
        + nonceBytes
        + b":00000001:"
        + cnonce
        + b":auth:"
        + HHA2.encode()
    )
    resp_hash.update(message)
    challengeResp = resp_hash.hexdigest()

    fullResp = 'charset=utf-8,username="{0}",realm="{1}",nonce="{2}",nc=00000001,cnonce="{3}",digest-uri="{4}",maxbuf=65536,response={5},qop=auth'.format(
        b64Username.decode(), realm, nonce, cnonce.decode(), digestURI, challengeResp
    )
    return fullResp.encode()


def extractSaslParams(rawServerChall):
    try:
        saslNonce = re.search('nonce="([^"]+)"', rawServerChall, re.IGNORECASE).group(1)
        realm = re.search('realm="([^"]+)"', rawServerChall, re.IGNORECASE).group(1)
        algorithm = re.search(
            "algorithm=(md5-sess)", rawServerChall, re.IGNORECASE
        ).group(1)
        return saslNonce, realm, algorithm
    except:
        return None, None, None


def getServerChallenge(sClient, sock):
    nonce = (
        b"\x00\x00\x00\x00\x00\x00\x00\x2b\x03\x41\x4d\x9e\xbf\x8c\x7e\x68"
        b"\x26\x00\x00\x00\x16"
    )
    helloSASL = (
        b"\xea\x00\x00\x00\x0d\x73\x70\x61\x72\x6b\x53\x61\x73\x6c\x55\x73"
        b"\x65\x72\x00\x00\x00\x00"
    )
    serverChallenge = sClient.sendRawMessage(nonce, helloSASL, sock, 0)
    if serverChallenge is None:
        return None
    return serverChallenge.decode("utf-8", "ignore")


def _setup_socket(sClient):
    server_address = (sClient.target, sClient.port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3)
    sock.connect(server_address)
    return sock


def sendChallenge(sClient, saslNonce, realm, secret, username, sock):
    resp = getSASLHash(saslNonce, realm, secret, username)
    payload = (
        b"\xea\x00\x00\x00\x0d\x73\x70\x61\x72\x6b\x53\x61\x73\x6c\x55\x73"
        b"\x65\x72\x00\x00\x01\x06"
    )
    payload += resp
    payloadSize = struct.pack(">I", len(payload))
    payloadSize15 = struct.pack(">I", len(payload) + 15)
    nonce = (
        b"\x00\x00\x00\x00"
        + payloadSize15
        + b"\x03\x68\xbd\x9f\xb5\x86\x94\xf2\x94"
        + payloadSize
    )
    serverResp = sClient.sendRawMessage(nonce, payload, sock, 0)
    return serverResp


def isSecretSaslValid(sClient, secret, username="sparkSaslUser", quiet=False):
    sock = _setup_socket(sClient)
    rawServerChall = getServerChallenge(sClient, sock)
    if rawServerChall is None:
        whine("Could not fetch server SASL challenge", "err")
        return None
    saslNonce, realm, algorithm = extractSaslParams(rawServerChall)
    if algorithm != "md5-sess":
        whine(
            "Cannot check the secret provided. Received weird algorithm %s "
            % str(algorithm)
        )
    if saslNonce is None or realm is None:
        whine("Got Null Nonce (%s) or Realm (%s)" % (str(saslNonce), str(realm)), "err")
        return None
    serverResp = sendChallenge(sClient, saslNonce, realm, secret, username, sock)
    if serverResp is None:
        whine("Could not authenticate to the Spark server", "err")
        return None
    serverResp = serverResp.decode("utf-8", "ignore")
    if "rspauth=" in serverResp:
        return secret
    elif "SaslException" in serverResp:
        saslException = re.search(
            "javax\.security\.sasl\.SaslException: ([\w\d -_.,]+)\n",
            serverResp,
            re.IGNORECASE,
        )
        if not quiet:
            whine("Got sasl exception from server when submitting challenge", "err")
            if saslException:
                whine(saslException.group(1), "err")
        return False
