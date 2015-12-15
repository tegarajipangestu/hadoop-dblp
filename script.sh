#!/bin/bash
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
mkdir bin
hadoop com.sun.tools.javac.Main -d src/main/java/com/hadoop/counter/DBLPDriver.java src/main/java/com/hadoop/counter/DBLPCounter.java 
cd bin
jar cf hadoop-dblp.jar DBLPDriver*.class DBLPCounter*.class
cd ..
hadoop jar bin/hadoop-dblp.jar DBLPDriver dblp/dblp-short.xml /user/tegarnization/dblp
