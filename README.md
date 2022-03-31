# dist-merge-sort

## Setup

### Prerequisites
- [Minikube](https://minikube.sigs.k8s.io/docs/start/)
- [Stern](https://github.com/wercker/stern)
- [JDK (8 or higher)](https://adoptopenjdk.net/installation.html)
- [Gradle](https://gradle.org/install/)
- [Node JS](https://nodejs.org/en/download/)

### Building the Program
```shell
# Change to kubernetes folder and deploy base infrastructure (namespace, zookeeper service)
cd k8s
./setup.sh
cd ../test-data
# Generate test data
npm run generate-data
cd ..
# Compiles/builds JAR and builds docker container with JAR in it
./bin/build_docker_container.sh
```

## Running the Program
There is a script that will deploy pods to k8s and start executing the distributed merge/sort.
This mounts local paths to minikube you will need to Ctrl+C to exit the process and turn off the mounts once the job is finished.
```shell
./bin/start_dist_merge_sort.sh
```

To view logs of the job while it's running:
```shell
./bin/view_job_logs.sh
```