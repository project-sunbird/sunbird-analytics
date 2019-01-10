node('build-slave') {
    try {
        String ANSI_GREEN = "\u001B[32m"
        String ANSI_NORMAL = "\u001B[0m"
        String ANSI_BOLD = "\u001B[1m"
        String ANSI_RED = "\u001B[31m"
        String ANSI_YELLOW = "\u001B[33m"

        if (params.size() == 0){
            properties([[$class: 'RebuildSettings', autoRebuild: false, rebuildDisabled: false], parameters([string(defaultValue: '', description: '<font color=teal size=2>If you want to build from a tag, specify the tag name. If this parameter is blank, latest commit hash will be used to build</font>', name: 'tag', trim: false)])])

            ansiColor('xterm') {
                println (ANSI_BOLD + ANSI_GREEN + '''\
                        First run of the job. Parameters created. Stopping the current build.
                        Please trigger new build and provide parameters if required.
                        '''.stripIndent().replace("\n"," ") + ANSI_NORMAL)
            }
            return
        }

        ansiColor('xterm') {
            stage('Checkout') {
                cleanWs()
                if(params.tag == ""){
                    checkout scm
                    commit_hash = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                    branch_name = sh(script: 'git name-rev --name-only HEAD | rev | cut -d "/" -f1| rev', returnStdout: true).trim()
                    artifact_version = branch_name + "_" + commit_hash
                }
                else {
                    def scmVars = checkout scm
                    checkout scm: [$class: 'GitSCM', branches: [[name: "refs/tags/$params.tag"]],  userRemoteConfigs: [[url: scmVars.GIT_URL]]]
                    artifact_version = params.tag
                }
                echo "artifact_version: "+ artifact_version
            }
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
                        zip -j lpa_artifacts.zip:${artifact_version} lpa_artifacts/*
                    """
                archiveArtifacts artifacts: "lpa_artifacts.zip:${artifact_version}", fingerprint: true, onlyIfSuccessful: true
                sh """echo {\\"artifact_name\\" : \\"lpa_artifacts.zip\\", \\"artifact_version\\" : \\"${artifact_version}\\", \\"node_name\\" : \\"${env.NODE_NAME}\\"} > metadata.json"""
                archiveArtifacts artifacts: 'metadata.json', onlyIfSuccessful: true
            }
        }

    catch (err) {
        currentBuild.result = "FAILURE"
        throw err
    }

}
