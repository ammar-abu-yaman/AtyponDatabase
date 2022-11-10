FROM openjdk:11
EXPOSE 8080
ADD target/worker-0.0.1-SNAPSHOT.jar worker-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java", "-jar", "/worker-0.0.1-SNAPSHOT.jar"]
