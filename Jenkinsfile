node('build-slave') {
    try {
        ansiColor('xterm') {
            stage('Checkout') {
                cleanWs()
            }

            stage('Build') {
                    sh """
                    cd platform-framework && mvn clean install -DskipTests=true
                    cd ../platform-modules && mvn clean install -DskipTests 
                    cd job-manager && mvn clean package
                    cd ../../platform-api && mvn clean install -DskipTests=true 
                    mvn play2:dist -pl analytics-api
                    """
            }

            stage('Archive artifacts'){
                commit_hash = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                branch_name = sh(script: 'git name-rev --name-only HEAD | rev | cut -d "/" -f1| rev', returnStdout: true).trim()
                artifact_version = branch_name + "_" + commit_hash
                sh """
                        mkdir lpa_artifacts
                        cp platform-framework/analytics-job-driver/target/analytics-framework-1.0.jar lpa_artifacts
                        cp platform-modules/batch-models/target/batch-models-1.0.jar lpa_artifacts
                        cp platform-modules/job-manager/target/job-manager-1.0-distribution.tar.gz lpa_artifacts
                        cp platform-api/analytics-api/target/analytics-api-1.0-dist.zip lpa_artifacts
                        zip -r lpa_artifacts.zip:${artifact_version} lpa_artifacts
                        rm -rf lpa_artifacts
                    """
                archiveArtifacts artifacts: "lpa_artifacts.zip:${artifact_version}", fingerprint: true, onlyIfSuccessful: true
                sh """echo {\\"artifact_name\\" : \\"lpa_artifacts.zip\\", \\"artifact_version\\" : \\"${artifact_version}\\", \\"node_name\\" : \\"${env.NODE_NAME}\\"} > metadata.json"""
                archiveArtifacts artifacts: 'metadata.json', onlyIfSuccessful: true
                sh "rm lpa_artifacts.zip:${artifact_version}"
            }
        }
    }

    catch (err) {
        currentBuild.result = "FAILURE"
        throw err
    }

}
