FROM maven:3-eclipse-temurin-21 AS build-stage

LABEL org.opencontainers.image.authors="Matthias Geerdsen <matthias.geerdsen@intranda.com>"
LABEL org.opencontainers.image.source="https://github.com/intranda/goobi-viewer-indexer"
LABEL org.opencontainers.image.description="Goobi viewer - Indexer daemon"

# you can use --build-arg build=false to skip viewer.war compilation, a viewer.war file needs to be available in target/viewer.war then
ARG build=true

COPY ./ /indexer
WORKDIR /indexer
RUN echo $build; if [ "$build" = "true" ]; then mvn -f goobi-viewer-indexer clean package; elif [ -f "/indexer/goobi-viewer-indexer/target/solr-Indexer.jar" ]; then echo "using existing indexer jar"; else echo "not supposed to build, but no indexer jar found either"; exit 1; fi 


# start assembling the final image
FROM eclipse-temurin:21-jre-alpine AS assemble-stage
LABEL org.opencontainers.image.authors="Matthias Geerdsen <matthias.geerdsen@intranda.com>"


ENV SOLR_HOST=solr
ENV VIEWER_HOST=viewer

RUN apk add --no-cache openjpeg bash curl && rm -rf /tmp/* /var/tmp/*

RUN mkdir -p /opt/digiverso/indexer && mkdir /indexer-template

COPY --from=build-stage  /indexer/goobi-viewer-indexer/target/solr-Indexer.jar /usr/local/bin/solrIndexer.jar
COPY goobi-viewer-indexer/src/main/resources/config_indexer.xml /indexer-template/
COPY ./docker/run.sh /

CMD ["/run.sh"]
