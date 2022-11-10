FROM openjdk:11
EXPOSE 4000
ADD target/worker-0.0.1-SNAPSHOT.jar worker-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java", "-jar", "/spring-0.0.1-SNAPSHOT.jar"]
