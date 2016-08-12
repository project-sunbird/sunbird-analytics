#basePath=/Users/amitBehera/github/ekStep/Learning-Platform-Analytics
basePath=/Users/soma/github/ekStep/Learning-Platform-Analytics
fwJar=$basePath/platform-framework/analytics-job-driver/target/analytics-framework-1.0.jar
scriptDir=$basePath/platform-scripts/RecoEngine

spark-shell -i $scriptDir/DataFetcherWithContent.scala --jars $fwJar