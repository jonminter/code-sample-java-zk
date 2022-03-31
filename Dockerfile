FROM openjdk:11-jre-slim
COPY build/libs/*-all.jar app.jar
ENTRYPOINT ["java", "-Dcom.sun.management.jmxremote.port=9999", "-Dcom.sun.management.jmxremote.rmi.port=9999", "-Dcom.sun.management.jmxremote.authenticate=false", "-Dcom.sun.management.jmxremote.ssl=false", "-Dcom.sun.management.jmxremote.local.only=false", "-Djava.rmi.server.hostname=127.0.0.1", "-jar", "app.jar"]