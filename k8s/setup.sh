#!/bin/bash
set -x
kubectl apply -f 00-namespace.yml
kubectl apply -f 01-zookeeper-service.yml
kubectl apply -f 02-zookeeper-deployment.yml
kubectl apply -f 03-zoo-navigator.yml
kubectl apply -f 04-redis.yml
set +x