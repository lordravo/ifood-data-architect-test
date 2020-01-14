gcloud beta dataproc clusters create pyspark-jupyter \
    --region us-central1 \
    --zone us-central1-a \
    --master-machine-type n1-standard-8 \
    --master-boot-disk-size 500 \
    --num-workers 2 \
    --worker-machine-type n1-standard-4 \
    --worker-boot-disk-size 500 \
    --image-version 1.3.48-debian9 \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --optional-components 'JUPYTER,ANACONDA' \
    --project $PROJECT_ID \
    --enable-component-gateway \
    --properties "\
core:fs.s3.awsAccessKeyId=$AWS_ACCESS_KEY_ID,\
core:fs.s3.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY,\
core:fs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY_ID,\
core:fs.s3n.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY"