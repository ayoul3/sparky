# SimpleApp

SimpleApp is the JAR payload that sparky executes in cluster mode.

## Build
First install java
```sh
sudo apt install -y openjdk-8-jdk
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

```
Download scala version 2.11
```sh
wget www.scala-lang.org/files/archive/scala-2.11.12.deb
sudo dpkg -i scala-2.11.12.deb
scala -version
Scala code runner version 2.11.12 -- Copyright 2002-2019, LAMP/EPFL and Lightbend, Inc.
```
Grab the jar file spark-core_2.11-2.4.3.jar from Spark's [website]([website](https://spark.apache.org/news/index.html)). Make sure Spark version matches that of the cluster you're targetting (2.4.3 in this case)

Package the code in a Jar file:

```sh
scalac -classpath "./spark-core_2.11-2.4.3.jar" SimpleApp.scala -d SimpleApp.jar
```




