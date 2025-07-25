pipeline {

  agent none

  options {
    buildDiscarder logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '15', daysToKeepStr: '90', numToKeepStr: '')
    disableConcurrentBuilds()
  }

  environment {
    GHCR_IMAGE_BASE = 'ghcr.io/intranda/goobi-viewer-indexer'
    DOCKERHUB_IMAGE_BASE = 'intranda/goobi-viewer-indexer'
    NEXUS_IMAGE_BASE = 'nexus.intranda.com:4443/goobi-viewer-indexer'
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
    stage('build and and publish docker image') {
      agent {label 'controller'}
      when {
        anyOf {
          tag "v*"
          branch 'develop'
          expression { return env.BRANCH_NAME =~ /_docker$/ }
        }
      }
      steps {
        unstash 'app'
        withCredentials([
          usernamePassword(
            credentialsId: 'jenkins-github-container-registry',
            usernameVariable: 'GHCR_USER',
            passwordVariable: 'GHCR_PASS'
          ),
          usernamePassword(
            credentialsId: '0b13af35-a2fb-41f7-8ec7-01eaddcbe99d',
            usernameVariable: 'DOCKERHUB_USER',
            passwordVariable: 'DOCKERHUB_PASS'
          ),
          usernamePassword(
            credentialsId: 'jenkins-docker',
            usernameVariable: 'NEXUS_USER',
            passwordVariable: 'NEXUS_PASS'
          )
        ]) {
          sh '''
            # Login to registries
            echo "$GHCR_PASS" | docker login ghcr.io -u "$GHCR_USER" --password-stdin
            echo "$DOCKERHUB_PASS" | docker login docker.io -u "$DOCKERHUB_USER" --password-stdin
            echo "$NEXUS_PASS" | docker login nexus.intranda.com:4443 -u "$NEXUS_USER" --password-stdin

            # Setup QEMU and Buildx
            docker buildx create --name multiarch-builder --use || docker buildx use multiarch-builder
            docker buildx inspect --bootstrap

            # Tag logic
            TAGS=""
            if [ ! -z "$TAG_NAME" ]; then
              TAGS="$TAGS -t $GHCR_IMAGE_BASE:$TAG_NAME -t $DOCKERHUB_IMAGE_BASE:$TAG_NAME -t $NEXUS_IMAGE_BASE:$TAG_NAME -t $GHCR_IMAGE_BASE:latest -t $DOCKERHUB_IMAGE_BASE:latest -t $NEXUS_IMAGE_BASE:latest"
            elif [ "$GIT_BRANCH" = "origin/develop" ] || [ "$GIT_BRANCH" = "develop" ]; then
              TAGS="$TAGS -t $GHCR_IMAGE_BASE:develop -t $DOCKERHUB_IMAGE_BASE:develop -t $NEXUS_IMAGE_BASE:develop"
            elif echo "$GIT_BRANCH" | grep -q "_docker$"; then
              TAG_SUFFIX=$(echo "$GIT_BRANCH" | sed 's/_docker$//' | sed 's|/|_|g')
              TAGS="$TAGS -t $GHCR_IMAGE_BASE:$TAG_SUFFIX -t $DOCKERHUB_IMAGE_BASE:$TAG_SUFFIX -t $NEXUS_IMAGE_BASE:$TAG_SUFFIX"
            else
              echo "No matching tag, skipping build."
              exit 0
            fi

            # Build and push to all registries
            docker buildx build --build-arg build=false \
              --no-cache \
              --platform linux/amd64,linux/arm64/v8,linux/ppc64le,linux/s390x \
              $TAGS \
              --push .
          '''
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
