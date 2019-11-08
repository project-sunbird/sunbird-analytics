node('build-slave') {
    try {
        String ANSI_GREEN = "\u001B[32m"
        String ANSI_NORMAL = "\u001B[0m"
        String ANSI_BOLD = "\u001B[1m"
        String ANSI_RED = "\u001B[31m"
        String ANSI_YELLOW = "\u001B[33m"
        ansiColor('xterm') {
            stage('Checkout') {
                cleanWs()
                if(params.github_release_tag == ""){
                    checkout scm
                    commit_hash = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                    branch_name = sh(script: 'git name-rev --name-only HEAD | rev | cut -d "/" -f1| rev', returnStdout: true).trim()
                    artifact_version = branch_name + "_" + commit_hash
            println(ANSI_BOLD + ANSI_YELLOW + "github_release_tag not specified, using the latest commit hash: " + commit_hash + ANSI_NORMAL)
                }
                else {
                    def scmVars = checkout scm
                    checkout scm: [$class: 'GitSCM', branches: [[name: "refs/tags/$params.github_release_tag"]],  userRemoteConfigs: [[url: scmVars.GIT_URL]]]
                    artifact_version = params.github_release_tag
            println(ANSI_BOLD + ANSI_YELLOW + "github_release_tag specified, building from github_release_tag: " + params.github_release_tag + ANSI_NORMAL)
                }
                echo "artifact_version: "+ artifact_version
            }
        }
        stage('Pre-Build') {
            sh '''
                sed -i "s#>logs<#>/mount/data/analytics/logs/api-service<#g" platform-api/analytics-api/conf/log4j2.xml
                sed -i 's#${application.home:-.}/logs#/mount/data/analytics/logs/api-service#g' platform-api/analytics-api/conf/logback.xml
                sed -i "s/cassandra.service.embedded.enable=false/cassandra.service.embedded.enable=true/g" platform-api/analytics-api/conf/application.conf
                sed -i "s/cassandra.service.embedded.enable=false/cassandra.service.embedded.enable=true/g" platform-api/analytics-api-core/src/test/resources/application.conf
                #sed -i "s/'replication_factor': '2'/'replication_factor': '1'/g" platform-scripts/database/data.cql
                rm -rf script
                mkdir script
                cp -r platform-scripts/python/main/BusinessMetrics script
                cp -r platform-scripts/python/main/vidyavaani script
                cp -r platform-scripts/VidyaVani/GenieSearch script
                cp -r platform-scripts/VidyaVani/VidyavaniCnQ script
                zip -r script.zip script
                '''
        }
        stage('Build') {
            sh '''
                cd platform-framework && mvn clean install -DskipTests=true
                cd ../platform-modules && mvn clean install -DskipTests
                cd job-manager && mvn clean package
                cd ../../platform-api && mvn clean install -DskipTests=true
                mvn play2:dist -pl analytics-api
                '''
        }
        stage('Archive artifacts'){
            sh """
                        mkdir lpa_artifacts
                        cp platform-framework/analytics-job-driver/target/analytics-framework-1.0.jar lpa_artifacts
                        cp platform-modules/batch-models/target/batch-models-1.0.jar lpa_artifacts
                        cp platform-modules/job-manager/target/job-manager-1.0-distribution.tar.gz lpa_artifacts
                        cp platform-api/analytics-api/target/analytics-api-1.0-dist.zip lpa_artifacts
                        cp script.zip lpa_artifacts
                        zip -j lpa_artifacts.zip:${artifact_version} lpa_artifacts/*
                    """
            archiveArtifacts artifacts: "lpa_artifacts.zip:${artifact_version}", fingerprint: true, onlyIfSuccessful: true
            sh """echo {\\"artifact_name\\" : \\"lpa_artifacts.zip\\", \\"artifact_version\\" : \\"${artifact_version}\\", \\"node_name\\" : \\"${env.NODE_NAME}\\"} > metadata.json"""
            archiveArtifacts artifacts: 'metadata.json', onlyIfSuccessful: true
            currentBuild.description = artifact_version
        }
    }
    catch (err) {
        currentBuild.result = "FAILURE"
        throw err
    }
}