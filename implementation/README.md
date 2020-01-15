# Implementation

Dataproc was chosen as a solution of architecture, since it is a cloud service which already implements Spark, Hadoop and a full set of operations to parameterize and scale up/down the cluster. Moreover, it's pretty easy to set, add components or run provision scripts.

## Requirements

ToDo

## Launching Cluster

* Export both GCP Project and AWS Credentials
```
export PROJECT_ID=<project_id>
export AWS_ACCESS_KEY_ID=<key_id>
export AWS_SECRET_ACCESS_KEY=<key_secret>
```

* Execute and wait for cluster creation:
```
bash launch_cluster.sh
```

ToDo: Initialization Script (git pull; copy jobs to hdfs)

## Submiting Job

Run submit_job.sh script passing the job_name. Example
```
bash submit_job.sh create_table_raw_consumer
```

## Reference

Dataproc: https://cloud.google.com/dataproc
SubmitJob: https://cloud.google.com/dataproc/docs/guides/submit-job