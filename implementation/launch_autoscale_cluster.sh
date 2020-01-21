gcloud beta dataproc clusters create pyspark-jupyter \
    --region us-central1 \
    --zone us-central1-a \
    --master-machine-type n1-standard-2 \
    --master-boot-disk-size 256 \
    --num-workers 3 \
    --worker-machine-type n1-highmem-2 \
    --worker-boot-disk-size 100 \
    --image-version 1.3.48-debian9 \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --optional-components 'JUPYTER,ANACONDA' \
    --project $PROJECT_ID \
    --enable-component-gateway \
    --metadata  SOURCE_REPO=$SOURCE_REPO \
    --initialization-actions $INIT_ACTION \
    --properties "\
core:fs.s3.awsAccessKeyId=$AWS_ACCESS_KEY_ID,\
core:fs.s3.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY,\
core:fs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY_ID,\
core:fs.s3n.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY,\
dataproc:alpha.autoscaling.enabled=true,\
dataproc:alpha.autoscaling.primary.max_workers=$MAX_WORKERS,\
dataproc:alpha.autoscaling.secondary.max_workers=0,\
dataproc:alpha.autoscaling.scale_up.factor=1,\
dataproc:alpha.autoscaling.graceful_decommission_timeout=2h,\
dataproc:alpha.autoscaling.cooldown_period=5m"