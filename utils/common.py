from .colors import Colors
import socket
import os, sys
import errno

toHex = lambda x: "".join([hex(ord(c))[2:].zfill(2) for c in x])


def whine(text, kind="clear", level=0):
    """
    Handles screen messages display
    """

    typeDisp, levelDisp, color = "", "", ""
    endColor = Colors.ENDC
    level = int(level)
    msgMap = {
        "warn": ["[!]", Colors.YELLOW],
        "info": ["[*]", Colors.BLUE],
        "err": ["[-]", Colors.RED],
        "good": ["[+]", Colors.GREEN],
        "clear": ["[ ]", ""],
    }

    typeDisp = msgMap.get(kind, ["[ ]", ""])[0]
    color = msgMap.get(kind, ["[ ]", ""])[1] if os.name != "nt" else ""
    endColor = Colors.ENDC if color != "" else ""

    levelDisp = "".join(["    "] * level)

    print(
        "{color}{level}{type}{endColor} {text}".format(
            color=color, level=levelDisp, type=typeDisp, text=text, endColor=endColor
        )
    )


def confirmSparkMaster(sparkSession):
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
        server_address = (sparkSession.target, sparkSession.port)
        sock.settimeout(3)
        sock.connect(server_address)
        sock.send(nonce)
        sock.send(hello)
        respNone = sock.recv(21)
        if len(respNone) == 21 and respNone[10] == nonce[10]:
            whine(
                "Spark master confirmed at {0}:{1}".format(
                    sparkSession.target, sparkSession.port
                ),
                "good",
            )
            return True
    except socket.error as serr:
        if serr.errno == errno.ECONNREFUSED:
            whine(serr, "err")
            sys.exit(-1)
        whine(serr, "warn")
        return False
