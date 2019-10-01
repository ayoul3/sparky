import sys, os
import zipfile
import re
import string
from multiprocessing import Pool
import time

try:
    from StringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO

listPatterns = {
    "AWS Access Key": "(AKIA[A-Za-z0-9/+=]{16,16})",
    "AWS Secret Key": "fs\.s3a\.secret.{1,20}([A-Za-z0-9/+=]{40})",
    "AWS Secret Key 2": "fs\.s3n\.awsSecret.{1,20}([A-Za-z0-9/+=]{40})",
    "AWS S3 Bucket": "s3[an]?://[A-Za-z0-9/.-]+",
}
compiledPatterns = {}
printable = set(
    [
        " ",
        "$",
        "(",
        ",",
        "0",
        "4",
        "8",
        "<",
        "@",
        "D",
        "H",
        "L",
        "P",
        "T",
        "X",
        "\\",
        "`",
        "d",
        "h",
        "l",
        "p",
        "t",
        "x",
        "|",
        "#",
        "'",
        "+",
        "/",
        "3",
        "7",
        ";",
        "?",
        "C",
        "G",
        "K",
        "O",
        "S",
        "W",
        "[",
        "_",
        "c",
        "g",
        "k",
        "o",
        "s",
        "w",
        "{",
        "\n",
        '"',
        "&",
        "*",
        ".",
        "2",
        "6",
        ":",
        ">",
        "B",
        "F",
        "J",
        "N",
        "R",
        "V",
        "Z",
        "^",
        "b",
        "f",
        "j",
        "n",
        "r",
        "v",
        "z",
        "~",
        "\t",
        "!",
        "%",
        ")",
        "-",
        "1",
        "5",
        "9",
        "=",
        "A",
        "E",
        "I",
        "M",
        "Q",
        "U",
        "Y",
        "]",
        "a",
        "e",
        "i",
        "m",
        "q",
        "u",
        "y",
        "}",
    ]
)


def applyRegex(filename, input):
    for key in compiledPatterns.keys():
        printableInput = filter(lambda x: x in printable, input)
        printableInput = "".join(printableInput)
        res = compiledPatterns[key].findall(printableInput)
        if len(res) > 0:
            print("%s: %s - %s" % (filename, key, res))


def zip_extract(binaryInput):
    in_memory_data = StringIO(binaryInput)
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    files = [i for i in file_obj.namelist()]
    return [file_obj.open(file).read().decode("utf-8", "ignore") for file in files]


def lookupPasswords(filename):
    fileb = open(filename, "rb").read()
    if filename.endswith(".jar"):
        for input in zip_extract(fileb):
            applyRegex(filename, input)
    else:
        applyRegex(filename, fileb)


if __name__ == "__main__":
    listFiles = []
    extension = sys.argv[1]
    if extension != "*" and len(extension) > 2:
        extension = extension[2:]
    for key in listPatterns.keys():
        compiledPatterns[key] = re.compile(listPatterns[key])

    path = "{0}/../../".format(os.getcwd())
    for root, dir, files in os.walk(path):
        for file in files:
            if extension == "*" or extension in file:
                listFiles.append(os.path.join(root, file))

    p = Pool()
    p.map(lookupPasswords, listFiles)
