from .colors import Colors
import os


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

    typeDisp = msgMap.get(kind)[0]
    color = msgMap.get(kind)[1] if os.name != "nt" else ""
    endColor = Colors.ENDC if color != "" else ""

    levelDisp = "".join(["    "] * level)

    print(
        "{color}{level}{type}{endColor} {text}".format(
            color=color, level=levelDisp, type=typeDisp, text=text, endColor=endColor
        )
    )
