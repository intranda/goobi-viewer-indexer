pipeline {

  agent none

  options {
    buildDiscarder logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '15', daysToKeepStr: '90', numToKeepStr: '')
    disableConcurrentBuilds()
  }

  stages {
    stage('build develop') {
      when {
        not {
          anyOf {
            branch 'master';
            tag "v*"
          }
        }
      }
      agent {
        docker {
          label 'controller'
          image 'nexus.intranda.com:4443/goobi-viewer-testing-index:latest'
          args '-v $HOME/.m2:/var/maven/.m2:z -v $HOME/.config:/var/maven/.config -v $HOME/.sonar:/var/maven/.sonar -u 1000 -ti -e _JAVA_OPTIONS=-Duser.home=/var/maven -e MAVEN_CONFIG=/var/maven/.m2'
          registryUrl 'https://nexus.intranda.com:4443/'
          registryCredentialsId 'jenkins-docker'
        }
      }
      steps {
        sh 'git clean -fdx'
        sh 'mvn -f goobi-viewer-indexer/pom.xml -DskipTests=false -DskipDependencyCheck=false -DskipCheckstyle=false clean verify -U'
        dependencyCheckPublisher pattern: '**/target/dependency-check-report.xml'
        stash includes: '**/target/*.jar, */src/main/resources/*.xml, */src/main/resources/other/schema.xml', name:  'app'
      }
    }

    stage('build release') {
      when {
        anyOf {
          branch 'master';
          tag "v*"
        }
      }
      agent {
        docker {
          label 'controller'
          image 'nexus.intranda.com:4443/goobi-viewer-testing-index:latest21'
          args '-v $HOME/.m2:/var/maven/.m2:z -v $HOME/.config:/var/maven/.config -v $HOME/.sonar:/var/maven/.sonar -u 1000 -ti -e _JAVA_OPTIONS=-Duser.home=/var/maven -e MAVEN_CONFIG=/var/maven/.m2'
          registryUrl 'https://nexus.intranda.com:4443/'
          registryCredentialsId 'jenkins-docker'
        }
      }
      steps {
        sh 'git clean -fdx'
        sh 'mvn -f goobi-viewer-indexer/pom.xml -DskipTests=false -DskipDependencyCheck=false -DskipCheckstyle=false -DfailOnSnapshot=true clean verify -U'
        dependencyCheckPublisher pattern: '**/target/dependency-check-report.xml'
        stash includes: '**/target/*.jar, */src/main/resources/*.xml, */src/main/resources/other/schema.xml', name:  'app'
      }
    }


    stage('sonarcloud') {
      when {
        anyOf {
          tag "v*"
          branch 'sonar_*'
        }
      }
      agent {
        docker {
          label 'controller'
          image 'nexus.intranda.com:4443/goobi-viewer-testing-index:latest21'
          args '-v $HOME/.m2:/var/maven/.m2:z -v $HOME/.config:/var/maven/.config -v $HOME/.sonar:/var/maven/.sonar -u 1000 -ti -e _JAVA_OPTIONS=-Duser.home=/var/maven -e MAVEN_CONFIG=/var/maven/.m2'
          registryUrl 'https://nexus.intranda.com:4443/'
          registryCredentialsId 'jenkins-docker'
        }
      }
      steps {
        withCredentials([string(credentialsId: 'jenkins-sonarcloud', variable: 'TOKEN')]) {
          sh 'mvn -f goobi-viewer-indexer/pom.xml verify sonar:sonar -Dsonar.token=$TOKEN'
        }
      }
    }
    stage('deployment to maven repository') {
      agent {
        docker {
          label 'controller'
          image 'nexus.intranda.com:4443/goobi-viewer-testing-index:latest21'
          args '-v $HOME/.m2:/var/maven/.m2:z -v $HOME/.config:/var/maven/.config -v $HOME/.sonar:/var/maven/.sonar -u 1000 -ti -e _JAVA_OPTIONS=-Duser.home=/var/maven -e MAVEN_CONFIG=/var/maven/.m2'
          registryUrl 'https://nexus.intranda.com:4443/'
          registryCredentialsId 'jenkins-docker'
        }
      }
      when {
        anyOf {
          tag "v*"
          branch 'develop'
        }
      }
      steps {
        sh 'mvn -f goobi-viewer-indexer/pom.xml deploy'
      }
    }
    stage('build, test and publish docker image') {
      agent {label 'controller'}
      when {
        anyOf {
          tag "v*"
          branch 'develop'
        }
      }
      steps {
        unstash 'app'

        script{
          docker.withRegistry('https://nexus.intranda.com:4443','jenkins-docker'){
            dockerimage = docker.build("goobi-viewer-indexer:${BRANCH_NAME}-${env.BUILD_ID}_${env.GIT_COMMIT}", "--no-cache --build-arg build=false .")
            dockerimage_public = docker.build("intranda/goobi-viewer-indexer:${BRANCH_NAME}-${env.BUILD_ID}_${env.GIT_COMMIT}",  "--build-arg build=false .")
          }
        }
        script {
          dockerimage.inside {
            sh 'test -f /usr/local/bin/solrIndexer.jar || ( echo "/usr/local/bin/solrIndexer.jar missing"; exit 1 )'
          }
        }
        script {
          docker.withRegistry('https://nexus.intranda.com:4443','jenkins-docker'){
            dockerimage.push("${env.BRANCH_NAME}-${env.BUILD_ID}_${env.GIT_COMMIT}")
            dockerimage.push("${env.BRANCH_NAME}")
          }
        }
      }
    }
    stage('publish docker production image to internal repository'){
      agent {label 'controller'}
      when { 
        tag "v*" 
      }
      steps{
        script {
          docker.withRegistry('https://nexus.intranda.com:4443','jenkins-docker'){
            dockerimage.push("${env.TAG_NAME}-${env.BUILD_ID}")
            dockerimage.push("${env.TAG_NAME}")
            dockerimage.push("latest")
          }
        }
      }
    }
    stage('publish develop image to Docker Hub'){
      agent {label 'controller'}
      when {
        branch 'develop'
      }
      steps{
        script{
          docker.withRegistry('','0b13af35-a2fb-41f7-8ec7-01eaddcbe99d'){
            dockerimage_public.push("${env.BRANCH_NAME}")
          }
        }
      }
    }
    stage('publish develop image to GitHub container registry'){
      agent {label 'controller'}
      when {
        branch 'develop'
      }
      steps{
        script{
          docker.withRegistry('https://ghcr.io','jenkins-github-container-registry'){
            dockerimage_public.push("${env.BRANCH_NAME}")
          }
        }
      }
    }
    stage('publish production image to Docker Hub'){
      agent {label 'controller'}
      when {
        tag "v*"
      }
      steps{
        script{
          docker.withRegistry('','0b13af35-a2fb-41f7-8ec7-01eaddcbe99d'){
            dockerimage_public.push("${env.TAG_NAME}")
            dockerimage_public.push("latest")
          }
        }
      }
    }
    stage('publish production image to GitHub container registry'){
      agent {label 'controller'}
      when {
        tag "v*"
      }
      steps{
        script{
          docker.withRegistry('https://ghcr.io','jenkins-github-container-registry'){
            dockerimage_public.push("${env.TAG_NAME}")
            dockerimage_public.push("latest")
          }
        }
      }
    }
  }
  post {
    always {
      node('controller') {
        junit "**/target/surefire-reports/*.xml"
        step([
          $class           : 'JacocoPublisher',
          execPattern      : 'goobi-viewer-indexer/target/jacoco.exec',
          classPattern     : 'goobi-viewer-indexer/target/classes/',
          sourcePattern    : 'goobi-viewer-indexer/src/main/java',
          exclusionPattern : '**/*Test.class'
        ])
        recordIssues (
          enabledForFailure: true, aggregatingResults: false,
          tools: [checkStyle(pattern: '**/target/checkstyle-result.xml', reportEncoding: 'UTF-8')]
        )
      }
    }
    success {
      node('controller') {
        archiveArtifacts artifacts: '**/target/*.jar, */src/main/resources/*.xml, */src/main/resources/other/solrindexer.service', fingerprint: true
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
