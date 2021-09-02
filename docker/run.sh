#!/bin/bash
set -e

[ -z "$CONFIGSOURCE" ] && CONFIGSOURCE="default"

set -u

#if [ -n "${WORKING_STORAGE:-}" ]
#then
  #CATALINA_TMPDIR="${WORKING_STORAGE}/goobi/jvmtemp"
  #mkdir -p "${CATALINA_TMPDIR}"
  #echo >> /usr/local/tomcat/bin/setenv.sh
  #echo "CATALINA_TMPDIR=${CATALINA_TMPDIR}" >> /usr/local/tomcat/bin/setenv.sh
#fi

case $CONFIGSOURCE in
  # s3)
  #   if [ -z "$AWS_S3_BUCKET" ]
  #   then
  #     echo "AWS_S3_BUCKET is required"
  #     exit 1
  #   fi
  #   echo "Pulling configuration from s3 bucket"
  #   aws s3 cp s3://$AWS_S3_BUCKET/viewer/config/ /opt/digiverso/viewer/config/ --recursive
  #   ;;
  folder)
    if [ -z "$CONFIG_FOLDER" ]
    then
      echo "CONFIG_FOLDER is required"
      exit 1
    fi

    if ! [ -d "$CONFIG_FOLDER" ]
    then
      echo "CONFIG_FOLDER: $CONFIG_FOLDER does not exists or is not a folder"
      exit 1
    fi

    echo "Copying configuration from local folder"
    [ -d "$CONFIG_FOLDER" ] && cp -arv "$CONFIG_FOLDER"/* /opt/digiverso/indexer/
    ;;

  *)
    echo "Keeping configuration"
    # set solrUrl and viewerUrl from environment variables (defaults are given in Dockerfile)
    [ -w /opt/digiverso/indexer/solr_indexerconfig.xml ] && sed -e "s|<solrUrl>.*</solrUrl>|<solrUrl>${SOLR_URL}</solrUrl>|" -e "s|<viewerUrl>.*</viewerUrl>|<viewerUrl>${VIEWER_URL}</viewerUrl>|" -i /opt/digiverso/indexer/solr_indexerconfig.xml || echo "Did not modify indexer configuration from environment."

    ;;
esac


echo "Starting application"
exec java -Dlog4j.configurationFile=/log4j2.xml -jar /usr/local/bin/solrIndexer.jar /opt/digiverso/indexer/solr_indexerconfig.xml
