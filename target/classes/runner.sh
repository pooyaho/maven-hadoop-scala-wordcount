#!/bin/sh
export  JAVA_HOME=/home/pooya/Programs/jdk1.7.0/jre
sh /etc/hadoop/hadoop-env.sh
rm -fR ~/Desktop/hadoop
#cp ../../../out/artifacts/mvn_hadoop_jar/mvn-hadoop.jar .
hadoop jar mvn-hadoop.jar ir.phsys.WordCount input/ ~/Desktop/hadoop