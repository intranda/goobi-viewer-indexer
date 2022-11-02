FROM maven:3.6-jdk-11 AS BUILD
# you can use --build-arg build=false to skip viewer.war compilation, a viewer.war file needs to be available in target/viewer.war then
ARG build=true

COPY ./ /indexer
WORKDIR /indexer
RUN echo $build; if [ "$build" = "true" ]; then mvn -f goobi-viewer-indexer clean package; elif [ -f "/indexer/goobi-viewer-indexer/target/solr-Indexer.jar" ]; then echo "using existing indexer jar"; else echo "not supposed to build, but no indexer jar found either"; exit 1; fi 


# start assembling the final image
FROM openjdk:11-jdk AS ASSEMBLE
LABEL org.opencontainers.image.authors="Matthias Geerdsen <matthias.geerdsen@intranda.com>"


ENV SOLR_URL http://solr:8983/solr/collection1
ENV VIEWER_URL http://viewer:8080/viewer

RUN apt-get update && \
	apt-get -y install libopenjp2-7 && \
	apt-get -y clean && \
	rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir -p /opt/digiverso/indexer

COPY --from=BUILD  /indexer/goobi-viewer-indexer/target/solr-Indexer.jar /usr/local/bin/solrIndexer.jar
COPY --from=BUILD  /indexer/goobi-viewer-indexer/src/main/resources/config_indexer.xml /opt/digiverso/indexer/config_indexer.xml
COPY ./docker/* /
RUN sed -e "s|<solrUrl>.*</solrUrl>|<solrUrl>${SOLR_URL}</solrUrl>|" -e 's|C:||g' -e "s|<viewerUrl>.*</viewerUrl>|<viewerUrl>${VIEWER_URL}</viewerUrl>|" -i /opt/digiverso/indexer/config_indexer.xml

# TODO: check for solr availability before start (wait-for-solr from solr image?)

CMD ["/run.sh"]
