#!/bin/bash

# Usage: debug_with_jconsole_jmx.sh [Job Pod Suffix]
# Example: debug_with_jconsole_jmx.sh 7m4sl
pod_suffix=$1

kubectl --namespace dist-merge-sort port-forward dist-merge-sort-$pod_suffix 9999 &
port_forward_pid=$!
jconsole 127.0.0.1:9999
kill -9 $port_forward_pid