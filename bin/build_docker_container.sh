#!/bin/bash

eval $(minikube docker-env)
./gradlew clean shadowJar
docker build -t jonminter/dist-merge-sort .