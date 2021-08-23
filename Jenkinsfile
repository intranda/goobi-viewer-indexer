pipeline {

  agent none

  options {
    buildDiscarder logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '15', daysToKeepStr: '90', numToKeepStr: '')
  }

  stages {
    stage('prepare') {
      agent any
      steps {
        sh 'git clean -fdx'
      }
    }
    stage('build') {
      agent {
        docker {
          image 'nexus.intranda.com:4443/goobi-viewer-testing-index:latest'
          args '-v $HOME/.m2:/var/maven/.m2:z -v $HOME/.config:/var/maven/.config -v $HOME/.sonar:/var/maven/.sonar -u 1000 -ti -e _JAVA_OPTIONS=-Duser.home=/var/maven -e MAVEN_CONFIG=/var/maven/.m2'
          registryUrl 'https://nexus.intranda.com:4443/'
          registryCredentialsId 'jenkins-docker'
        }
      }
      steps {
              sh 'mvn -f goobi-viewer-indexer/pom.xml -DskipTests=false clean install -U'
              recordIssues enabledForFailure: true, aggregatingResults: true, tools: [java(), javaDoc()]
      }
    }
    stage('sonarcloud') {
      when {
        anyOf {
          branch 'sonar_*'
        }
      }
      agent {
        docker {
          image 'nexus.intranda.com:4443/goobi-viewer-testing-index:latest'
          args '-v $HOME/.m2:/var/maven/.m2:z -v $HOME/.config:/var/maven/.config -v $HOME/.sonar:/var/maven/.sonar -u 1000 -ti -e _JAVA_OPTIONS=-Duser.home=/var/maven -e MAVEN_CONFIG=/var/maven/.m2'
          registryUrl 'https://nexus.intranda.com:4443/'
          registryCredentialsId 'jenkins-docker'
        }
      }
      steps {
        withCredentials([string(credentialsId: 'jenkins-sonarcloud', variable: 'TOKEN')]) {
          sh 'mvn -f goobi-viewer-indexer/pom.xml verify sonar:sonar -Dsonar.login=$TOKEN'
        }
      }
    }
    stage('deployment to maven repository') {
      agent {
        docker {
          image 'nexus.intranda.com:4443/goobi-viewer-testing-index:latest'
          args '-v $HOME/.m2:/var/maven/.m2:z -v $HOME/.config:/var/maven/.config -v $HOME/.sonar:/var/maven/.sonar -u 1000 -ti -e _JAVA_OPTIONS=-Duser.home=/var/maven -e MAVEN_CONFIG=/var/maven/.m2'
          registryUrl 'https://nexus.intranda.com:4443/'
          registryCredentialsId 'jenkins-docker'
        }
      }
      when {
        anyOf {
        branch 'master'
        branch 'develop'
        }
      }
      steps {
        sh 'mvn -f goobi-viewer-indexer/pom.xml deploy'
      }
    }
    stage('build docker image') {
      agent any
      steps {
        script{
          docker.withRegistry('https://nexus.intranda.com:4443','jenkins-docker'){
            indexerimage = docker.build("goobi-viewer-indexer:${BRANCH_NAME}-${env.BUILD_ID}_${env.GIT_COMMIT}")
            indexerimage_public = docker.build("intranda/goobi-viewer-indexer:${BRANCH_NAME}-${env.BUILD_ID}_${env.GIT_COMMIT}")
          }
        }
      }
    }
    stage('basic tests'){
      agent any
      steps{
        script {
          indexerimage.inside {
            sh 'test -f  /opt/digiverso/indexer/solrIndexer.jar || echo "/opt/digiverso/indexer/solrIndexer.jar missing"'
          }
        }
      }
    }
    stage('publish docker devel image to internal repository'){
      agent any
      steps{
        script {
          docker.withRegistry('https://nexus.intranda.com:4443','jenkins-docker'){
            indexerimage.push("${env.BRANCH_NAME}-${env.BUILD_ID}_${env.GIT_COMMIT}")
            indexerimage.push("${env.BRANCH_NAME}")
          }
        }
      }
    }
    stage('publish develop image to Docker Hub'){
      agent any
      when {
        branch 'develop'
      }
      steps{
        script{
          docker.withRegistry('','0b13af35-a2fb-41f7-8ec7-01eaddcbe99d'){
            indexerimage_public.push("${env.BRANCH_NAME}")
          }
        }
      }
    }
    stage('publish docker production image to internal repository'){
      agent any
      when { branch 'master' }
      steps{
        script {
          docker.withRegistry('https://nexus.intranda.com:4443','jenkins-docker'){
            indexerimage.push("${env.TAG_NAME}-${env.BUILD_ID}_${env.GIT_COMMIT}")
            indexerimage.push("latest")
          }
        }
      }
    }
    stage('publish production image to Docker Hub'){
      agent any
      when {
        branch 'master'
      }
      steps{
        script{
          docker.withRegistry('','0b13af35-a2fb-41f7-8ec7-01eaddcbe99d'){
            indexerimage_public.push("${env.TAG_NAME}")
            indexerimage_public.push("latest")
          }
        }
      }
    }
  }
  post {
    always {
      node(null) {
        junit "**/target/surefire-reports/*.xml"
        step([
          $class           : 'JacocoPublisher',
          execPattern      : 'goobi-viewer-indexer/target/jacoco.exec',
          classPattern     : 'goobi-viewer-indexer/target/classes/',
          sourcePattern    : 'goobi-viewer-indexer/src/main/java',
          exclusionPattern : '**/*Test.class'
        ])
      }
    }
    success {
      node(null){
        archiveArtifacts artifacts: '**/target/*.jar, */src/main/resources/indexerconfig_solr.xml, */src/main/resources/other/schema.xml, */src/main/resources/other/solrindexer.service', fingerprint: true
      }
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
