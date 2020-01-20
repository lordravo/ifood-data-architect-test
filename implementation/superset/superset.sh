#!/bin/bash

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
    if [[ "${ROLE}" == 'Master' ]]; then
    mkdir superset
    cd superset

    apt-get update -y
    apt-get install -y \
      build-essential \
      libssl-dev \
      libffi-dev \
      libsasl2-dev \
      libldap2-dev \
      checkinstall \
      libreadline-gplv2-dev \
      libncursesw5-dev \
      libsqlite3-dev \
      tk-dev \
      libgdbm-dev \
      libc6-dev \
      libbz2-dev \
      libsasl2-modules \
      libsasl2-dev

    # Install Python3.6 for Superset
    wget https://www.python.org/ftp/python/3.6.0/Python-3.6.0.tar.xz
    tar xvf Python-3.6.0.tar.xz
    cd Python-3.6.0/
    ./configure
    make altinstall

    export PYTHONPATH=$(which python3.6)

    virtualenv -p python3.6 env_superset
    source env_superset/bin/activate

    # Install Superset
    env_superset/bin/pip3.6 install superset

    # Hive Dependencies
    env_superset/bin/pip3.6 install pyhive
    env_superset/bin/pip3.6 install thrift
    env_superset/bin/pip3.6 install sasl
    env_superset/bin/pip3.6 install thrift_sasl

    # Launch
    SUPERSET_MAIL=$(/usr/share/google/get_metadata_value attributes/SUPERSET_MAIL)
    SUPERSET_USER=$(/usr/share/google/get_metadata_value attributes/SUPERSET_USER)
    SUPERSET_PASS=$(/usr/share/google/get_metadata_value attributes/SUPERSET_PASS)
    SUPERSET_FNAME=$(/usr/share/google/get_metadata_value attributes/SUPERSET_FNAME)
    SUPERSET_LNAME=$(/usr/share/google/get_metadata_value attributes/SUPERSET_LNAME)
    SUPERSET_PORT=$(/usr/share/google/get_metadata_value attributes/SUPERSET_PORT)


    # Prepare SupersetConfig
    SUPERSET_CONFIG=$(/usr/share/google/get_metadata_value attributes/SUPERSET_CONFIG)
    cd /superset
    gsutil cp ${SUPERSET_CONFIG} superset_config.py
    export SUPERSET_CONFIG_PATH=/superset/superset_config.py

    fabmanager create-admin --app superset --username ${SUPERSET_USER} --firstname ${SUPERSET_FNAME} --lastname ${SUPERSET_LNAME} --email ${SUPERSET_MAIL} --password ${SUPERSET_PASS}

    superset db upgrade
    superset init

    nohup gunicorn -b 0.0.0.0:${SUPERSET_PORT} --limit-request-line 0 --limit-request-field_size 0 superset:app >> superset.log &
fi