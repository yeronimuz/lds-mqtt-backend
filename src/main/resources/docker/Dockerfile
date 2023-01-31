FROM anapsix/alpine-java
MAINTAINER jeroenlankheet <yeronimuz@gmail.com>

WORKDIR /app

# Copy jar and application.yml to docker build directory!
# application.yml host's mqtt-broker is at 172.17.0.1. This is a temp workaround until mosquitto is in it's own container

ADD application.yml /app
ADD ../build/libs/lnb-domotics-backend-0.4-RC.jar /app

CMD ["java", "-jar", "/app/lnb-domotics-backend-0.4-RC.jar", "/app/application.yml"]