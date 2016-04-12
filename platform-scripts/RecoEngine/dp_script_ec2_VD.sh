#!/bin/bash
set -e
cwd=`pwd`

runFramework=false
runBatchmodel=false
cleanTables=true

# github base path
basePath=/home/ec2-user

# batch model Jar file
modelJar=/mnt/data/analytics/models/batch-models-1.0.jar

# framework Jar file
fwJar=/mnt/data/analytics/models/analytics-framework-0.5.jar

# recommendation engine sprak-scala scripts path
scriptDir=$basePath/scripts/RecoEngine

# build mvn projects (in a subshell without changing directories)
if [ "$runFramework" = true ]; then
	echo "###### Building Analytics Frame Work ######"
	(cd $basePath/platform-framework; mvn clean install -DskipTests=true)
fi

if [ "$runBatchmodel" = true ]; then
	echo "#### Building Model ####"
	(cd $basePath/platform-modules/batch-models; mvn clean install -DskipTests=true)
fi


# echo "---- Deploying Project Jars ----"
# following copies are not necessary
# cp /Users/soma/github/ekStep/Learning-Platform-Analytics/platform-modules/batch-models/target/batch-models-1.0.jar /Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/RecoEngine/model
# cp /Users/soma/github/ekStep/Learning-Platform-Analytics/platform-framework/analytics-job-driver/target/analytics-framework-0.5.jar /Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/RecoEngine/model/

if [ "$cleanTables" = true ]; then
	echo "#### Clearing All Required Tables in 'learner_db' ####"
	#cd /Users/soma/apache-cassandra-2.2.5/bin
	cqlsh -f $scriptDir/queries.txt -k learner_db
fi


echo "Running Recommendation Engine"
spark-shell -i $scriptDir/RunRE.scala --jars $modelJar,$fwJar, --conf spark.cassandra.connection.host=127.0.0.1 spark.default.parallelism=4

# neo4j (IP)
# http://localhost:7474/browser/

# populate neo4j 
python $scriptDir/populateGraphDB.python

# create mock Concept-Content coverage
python $scriptDir/mockConceptCoverageGraphDB.python



