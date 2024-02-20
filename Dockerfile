FROM maven:3-eclipse-temurin-21 AS build
COPY ./src /tmp/src
COPY ./pom.xml /tmp/pom.xml
WORKDIR /tmp
RUN --mount=type=cache,target=/root/.m2,source=/root/.m2,from=ghcr.io/scc-digitalhub/openmetadata-java-connector:cache \ 
    mvn package -DskipTests

FROM gcr.io/distroless/java21-debian12:nonroot
ENV APP=OpenMetadataConnector-1.0.jar
LABEL org.opencontainers.image.source=https://github.com/scc-digitalhub/digitalhub-core
COPY --from=build /tmp/target/*.jar /app/${APP}
EXPOSE 8080
CMD ["/app/OpenMetadataConnector-1.0.jar"]