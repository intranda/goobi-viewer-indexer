pipeline {

  agent {
    docker {
      image 'nexus.intranda.com:4443/goobi-viewer-testing-index:latest'
      args '-v $HOME/.m2:/var/maven/.m2:z -u 1000 -ti -e _JAVA_OPTIONS=-Duser.home=/var/maven -e MAVEN_CONFIG=/var/maven/.m2'
      registryUrl 'https://nexus.intranda.com:4443/'
      registryCredentialsId 'jenkins-docker'
    }
  }

  options {
    buildDiscarder logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '15', daysToKeepStr: '90', numToKeepStr: '')
  }

  stages {
    stage('prepare') {
      steps {
        sh 'git clean -fdx'
      }
    }
    stage('build') {
      steps {
              sh 'mvn -f goobi-viewer-indexer/pom.xml -DskipTests=false clean install -U'
              recordIssues enabledForFailure: true, aggregatingResults: true, tools: [java(), javaDoc()]
      }
    }
    stage('deployment to maven repository') {
      when {
        anyOf {
        branch 'master'
        branch 'v*.*.*'
        }
      }
      steps {
        sh 'mvn -f goobi-viewer-indexer/pom.xml deploy'
      }
    }
  }
  post {
    always {
      junit "**/target/surefire-reports/*.xml"
    }
    success {
      archiveArtifacts artifacts: '**/target/*.jar, */src/main/resources/indexerconfig_solr.xml, */src/main/resources/other/schema.xml, */src/main/resources/other/solrindexer.service', fingerprint: true
    }
    changed {
      emailext(
        subject: '${DEFAULT_SUBJECT}',
        body: '${DEFAULT_CONTENT}',
        recipientProviders: [requestor(),culprits()],
        attachLog: true
      )
    }
  }
}
/* vim: set ts=2 sw=2 tw=120 et :*/
