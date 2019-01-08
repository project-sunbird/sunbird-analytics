node('build-slave') {

    try {
        
       stage('Checkout'){
          checkout scm
       }

       stage('Build Assets'){
          sh '''cd platform-framework && mvn clean install -DskipTests=true
           cd platform-modules && mvn clean install -DskipTests
           cd platform-api && mvn clean install -DskipTests=true
           cd platform-api && mvn play2:dist -pl analytics-api
          '''
        }
       
       stage('Archving Artifact'){
           archiveArtifacts("$WORKSPACE/platform-modules/batch-models/target/batch-models-1.0.jar, $WORKSPACE/platform-modules/job-manager/target/job-manager-1.0-distribution.tar.gz, $WORKSPACE/cp platform-api/analytics-api/target/analytics-api-1.0-dist.zip")
       }
    }

    catch (err) {
        throw err
    }
}
