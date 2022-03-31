#!/bin/bash

echo "Deploying/starting app in k8s..."
kubectl apply -f k8s/05-dist-merge-sort-job.yml