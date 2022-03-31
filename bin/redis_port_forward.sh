#!/bin/bash

pod_suffix=$1

kubectl --namespace dist-merge-sort port-forward redis-$pod_suffix 6379