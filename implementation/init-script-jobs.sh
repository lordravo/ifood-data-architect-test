#!/bin/bash

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
    mkdir -p ifood_etl
    cd /ifood_etl

    SOURCE_REPO=$(/usr/share/google/get_metadata_value attributes/SOURCE_REPO)
    git clone ${SOURCE_REPO}
    REPO_FOLDER=$(basename ${SOURCE_REPO} | awk '{gsub(/\.git/, ""); print}')
    JOBS_PATH=$(find "$REPO_FOLDER" -type d -name jobs)

    hadoop fs -copyFromLocal -f "$JOBS_PATH" /jobs
fi