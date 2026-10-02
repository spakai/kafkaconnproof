FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY java/ .
RUN mvn -B -q package dependency:copy-dependencies -DincludeScope=runtime
ARG DATASTAX_VERSION=1.7.6
ARG DATASTAX_SHA256=83db4a8b7edbd73c4cd17c151718c88a5dc33e34d90f1ed80e7f632d75dc8d25
RUN mkdir /plugin && curl -fSL --retry 3 "https://github.com/datastax/kafka-sink/releases/download/${DATASTAX_VERSION}/kafka-connect-cassandra-sink-${DATASTAX_VERSION}.jar" -o /plugin/datastax.jar \
 && echo "${DATASTAX_SHA256}  /plugin/datastax.jar" | sha256sum -c -
FROM confluentinc/cp-kafka-connect:7.8.0 AS connect
COPY --from=build /plugin/ /usr/share/java/rerate/
COPY --from=build /build/target/rerate-poc-1.0.jar /usr/share/java/rerate/
FROM eclipse-temurin:17-jre AS projection
WORKDIR /app
COPY --from=build /build/target/rerate-poc-1.0.jar /app/
COPY --from=build /build/target/dependency/ /app/lib/
ENTRYPOINT ["java", "-cp", "/app/rerate-poc-1.0.jar:/app/lib/*", "poc.Projection"]
