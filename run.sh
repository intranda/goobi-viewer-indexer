#!/bin/bash
set -e
set -u

# set solrUrl and viewerUrl from environment variables (defaults are given in Dockerfile) 
sed -e "s|<solrUrl>.*</solrUrl>|<solrUrl>${SOLR_URL}</solrUrl>|" -e "s|<viewerUrl>.*</viewerUrl>|<viewerUrl>${VIEWER_URL}</viewerUrl>|" -i /opt/digiverso/indexer/solr_indexerconfig.xml

echo "Starting application"
exec java -jar /opt/digiverso/indexer/solrIndexer.jar /opt/digiverso/indexer/solr_indexerconfig.xml