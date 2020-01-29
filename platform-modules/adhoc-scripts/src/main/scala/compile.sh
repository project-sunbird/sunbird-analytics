export SPARK_HOME=/mount/data/analytics/spark-2.0.1-bin-hadoop2.7
export MODELS_HOME=/mount/data/analytics
CLASSPATH=$(find $SPARK_HOME/jars -name '*.jar' | xargs echo | tr ' ' ':')
MODEL_CLASSPATH=$(find $MODELS_HOME/models -name '*.jar' | xargs echo | tr ' ' ':')
/mount/data/analytics/scala-2.11.8/bin/scalac $1.scala -classpath "$MODEL_CLASSPATH:$CLASSPATH" -d $1.jar
