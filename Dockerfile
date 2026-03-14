FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /build

COPY pom.xml .
COPY mvnw .
COPY mvnw.cmd .
COPY .mvn .mvn

RUN mvn dependency:go-offline -B

COPY src ./src

RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY --from=builder /build/bin/SimRunner.jar /app/simrunner.jar

RUN addgroup -S simrunner && adduser -S simrunner -G simrunner
USER simrunner

EXPOSE 3000 9090

ENTRYPOINT ["java", "-jar", "/app/simrunner.jar"]
CMD ["/config/workload.json"]
