gcloud dataproc jobs submit pyspark \
    --cluster pyspark-jupyter \
    --region us-central1 \
    hdfs:///jobs/$1.py