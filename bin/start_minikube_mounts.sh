#!/bin/bash

cleanup_mounts() {
  pid1=$1
  pid2=$2
  echo "Killing PID $pid1..."
  kill -9 $pid1
  echo "Killing PID $pid2..."
  kill -9 $pid2
}

echo "Mounting local path ./output in minikube..."
mkdir -p output
minikube mount $(pwd)/output:/data/dist-merge-sort/output &
mount_output_pid=$!
minikube mount $(pwd)/test-data/out:/data/dist-merge-sort/input &
mount_input_pid=$!
echo "Paths mounted. Hit Crtl-C to quit and stop mounts..."
wait