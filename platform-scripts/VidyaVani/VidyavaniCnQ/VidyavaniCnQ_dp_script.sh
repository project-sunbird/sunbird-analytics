#!/bin/bash
set -e
cwd=`pwd`

cleanTables=$1

# dev: 54.169.228.223 base path
basePath=/mnt/data/analytics/models

# batch model Jar file path
modelJar=$basePath/batch-models-1.0.jar

# framework Jar file path
fwJar=$basePath/analytics-framework-1.0.jar

# recommendation engine sprak-scala scripts path
scriptDir=/mnt/data/analytics/VidyavaniCnQ


if [ "$cleanTables" = true ]; then
	echo "#### Clearing All Required Tables in 'learner_db' ####"
	#cd /Users/soma/apache-cassandra-2.2.5/bin
	cqlsh -f $scriptDir/queries.txt -k learner_db
fi


echo "Running Recommendation Engine"
spark-shell -i $scriptDir/VidyavaniCnQRunLP.scala --jars $modelJar,$fwJar, --conf spark.cassandra.connection.host=127.0.0.1 spark.default.parallelism=4

# neo4j (IP)
# http://localhost:7474/browser/

