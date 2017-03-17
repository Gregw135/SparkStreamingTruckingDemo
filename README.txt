To run:

1. Make sure Kafka, Spark, Hive and HDFS are turned on.
2. Use Kafka CLI to create these topics: trucking, traffic, joined, windowed, trucking_predictions. /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list  sandbox.hortonworks.com:6667  --topic test
3. Call sbt assembly in the root folder to compile to the folder "target"
"4. Run /usr/hdp/current/spark-client/bin/spark-submit --class "main.scala.Collect --master local[4] ./SparkStreaming-assembly-2.0.0.jar
This runs Spark and performs joins, windowing and predictions on data received from Kafka. Output data is stored in HDFS and returned to Kafka.
5. To simulate input to Kafka run /usr/hdp/current/spark-client/bin/spark-submit --class "main.scala.Collect --master local[4] ./SparkStreaming-assembly-2.0.0.jar
6. Import the notebook into Zeppelin. Run the notebook cells in order to build a predictive model and export it to HDFS. This model will be imported the next time you run Collect.

To debug, first run this command on the sandbox:
export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8086

Then in Intellij go to Run > Edit Configurations. Hit + to add a new
configuration called SparkDebug. Set the host to 127.0.0.1 and port to 8086.
Immediately after running spark-submit on the sandbox hit debug in Intellij.
This should attach a remote debugger which will let you walk through the code
line by line. 
