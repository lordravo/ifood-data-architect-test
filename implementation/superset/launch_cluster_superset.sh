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
    --metadata  "\
SOURCE_REPO=$SOURCE_REPO,\
SUPERSET_MAIL=$SUPERSET_MAIL,\
SUPERSET_USER=$SUPERSET_USER,\
SUPERSET_PASS=$SUPERSET_PASS,\
SUPERSET_FNAME=$SUPERSET_FNAME,\
SUPERSET_LNAME=$SUPERSET_LNAME,\
SUPERSET_PORT=$SUPERSET_PORT,\
SUPERSET_CONFIG=$SUPERSET_CONFIG" \
    --initialization-actions "\
$INIT_ACTION,\
$SUPERSET_INIT" \
    --properties "\
core:fs.s3.awsAccessKeyId=$AWS_ACCESS_KEY_ID,\
core:fs.s3.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY,\
core:fs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY_ID,\
core:fs.s3n.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY"