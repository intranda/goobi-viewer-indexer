#!/bin/bash
set -e

[ -z "$CONFIGSOURCE" ] && CONFIGSOURCE="default"

set -u

if ! [[ -v SOLR_URL ]]; then
  export SOLR_URL=http://${SOLR_HOST}:8983/solr/current
  export SOLR_ADMIN=http://${SOLR_HOST}:8983/solr/admin
else
  export SOLR_ADMIN="${SOLR_URL%/*}/admin"
fi

if ! [[ -v VIEWER_URL ]]; then
  export VIEWER_URL=http://${VIEWER_HOST}:8080/viewer
fi

until curl --silent --fail "${SOLR_ADMIN}/collections?action=LISTALIASES" \
      | grep -q 'current'; do
    echo "Waiting for Solr..."
    sleep 5
done

echo "Solr ready!"

case $CONFIGSOURCE in
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
    ;;
esac


echo "Starting application"
exec java -jar /usr/local/bin/solrIndexer.jar /opt/digiverso/indexer/config_indexer.xml
