export SPARK_HOME=/Users/Santhosh/EkStep/spark-2.0.1-bin-hadoop2.7
CLASSPATH=$(find $SPARK_HOME/jars -name '*.jar' | xargs echo | tr ' ' ':')
MODEL_CLASSPATH=$(find $SPARK_HOME/models -name '*.jar' | xargs echo | tr ' ' ':')
scalac TestSparkSessionClose.scala -classpath "$MODEL_CLASSPATH:$CLASSPATH" -d TestSparkSessionClose.jar
$SPARK_HOME/bin/spark-submit --master local[*] --jars $SPARK_HOME/models/analytics-framework-1.0.jar,$SPARK_HOME/models/batch-models-1.0.jar --class TestSparkSessionClose TestSparkSessionClose.jar 