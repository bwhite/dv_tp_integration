mkdir wordcount_classes
javac -classpath ${HADOOP_HOME}/hadoop-core.jar:${HADOOP_HOME}/lib/commons-cli-1.2.jar -d wordcount_classes WordCount.java
jar -cvf wordcount.jar -C wordcount_classes/ .