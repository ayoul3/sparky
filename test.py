import subprocess


scriptInput = open("./utils/cloud.py", "r").read()
out = subprocess.Popen(
    ["python2", "-c", scriptInput], stdout=subprocess.PIPE
).stdout.read()
print(out)
