node('build-slave') {

    try {
        
       stage('Checkout'){
          checkout scm
       }

       stage('Build Assets'){
          sh '''cd platform-framework && mvn clean install -DskipTests=true
           cd ../platform-modules && mvn clean install -DskipTests 
           cd job-manager && mvn clean package
           cd ../platform-api && mvn clean install -DskipTests=true 
           mvn play2:dist -pl analytics-api
          '''
        }
       
       stage('Archving Artifact'){
           archiveArtifacts("platform-framework/analytics-job-driver/target/analytics-framework-1.0.jar, platform-modules/batch-models/target/batch-models-1.0.jar, platform-modules/job-manager/target/job-manager-1.0-distribution.tar.gz, platform-api/analytics-api/target/analytics-api-1.0-dist.zip")
       }
    }

    catch (err) {
        throw err
    }
}
