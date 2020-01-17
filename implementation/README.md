# Implementation

Dataproc was chosen as a solution of architecture, since it is a cloud service which already implements Spark, Hadoop and a full set of operations to parameterize and scale up/down the cluster. Moreover, it's pretty easy to set, add components or run provision scripts.

## Requirements

ToDo

## Launching Cluster

* Ensure the initialization script is on Cloud Storage
Currently, dataproc supports only gs:// paths for initialization scripts.
Upload to CloudStorage and add its path to variable INIT_ACTION

ToDo: Add cloudbuild.yaml to automatically rsync the init action on cloud storage

* Export both GCP Project, AWS Credentials, SOURCE_REPO AND INIT_ACTION
```
export PROJECT_ID=<project_id>
export AWS_ACCESS_KEY_ID=<key_id>
export AWS_SECRET_ACCESS_KEY=<key_secret>
export SOURCE_REPO=https://github.com/lordravo/ifood-data-architect-test.git
export INIT_ACTION=<init_action_gs_path>
```

* Execute and wait for cluster creation:
```
bash launch_cluster.sh
```

* Autoscaling
Use launch_autoscale_cluster.sh instead. Be sure to export MAX_WORKERS first

## Submiting Job

Run submit_job.sh script passing the job_name. Example
```
bash submit_job.sh create_table_raw_consumer
```

## Reference

Dataproc: https://cloud.google.com/dataproc
SubmitJob: https://cloud.google.com/dataproc/docs/guides/submit-job